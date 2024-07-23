import React, { useState, useEffect } from "react";
import Modal from "antd/lib/modal";
import * as Grid from "antd/lib/grid";
import notification from "antd/lib/notification";
import  Select from 'antd/lib/select';
import { wrap as wrapDialog, DialogPropType } from "@/components/DialogWrapper";
import PropTypes from "prop-types";
import Tooltip from "antd/lib/tooltip";
// eslint-disable-next-line import/no-extraneous-dependencies
import axios from "axios";
import { formatDateTime } from "@/lib/utils";

function EditDestinationDialog({ dialog, query }) {
  const [showForm, setShowForm] = useState(false);
  const [editForm, setEditForm] = useState(false);
  const [currentDestination, setCurrentDestination] = useState({ name: "", type: "gsheets", options: { spreadsheet_id: "", sheet_name: "", row: 1, column: "A" } });
  const [allDestinations, setAllDestinations] = useState([]);
  const [isLoaded, setIsLoaded] = useState(false);
  const [errorWhileLoading, setErrorWhileLoading] = useState(false);
  const [modalType, setModalType] = useState("");
  // eslint-disable-next-line max-len
  const [visualizationIDAndName, setVisualizationIDAndName] = useState({ id: query.visualizations[0].id, name: query.visualizations[0].name });
  const [syncDestination, setSyncDestination] = useState("");
  const [syncDisabled, setSyncDisabled] = useState(false);
  const { confirm } = Modal;


  const handleDestinationName = (event) => {
    const val = event.target.value;
    setCurrentDestination(prevState => ({ ...prevState, name: val }));
  };


  const handleSheetID = (event) => {
    const val = event.target.value;
    setCurrentDestination(prevState => ({
      ...prevState,
      options: {
        ...prevState.options,
        spreadsheet_id: val,
      },
    }));
  };


  const handleSheetName = (event) => {
    const val = event.target.value;
    setCurrentDestination(prevState => ({
      ...prevState,
      options: {
        ...prevState.options,
        sheet_name: val,
      },
    }));
  };


  const handleCellRow = (event) => {
    const row = event.target.value;
    setCurrentDestination(prevState => ({
      ...prevState,
      options: {
        ...prevState.options,
        row,
      },
    }));
  };


  const handleCellColumn = (event) => {
    const column = event.target.value;
    setCurrentDestination(prevState => ({
      ...prevState,
      options: {
        ...prevState.options,
        column,
      },
    }));
  };


  const handleVisualizationID = (event) => {
    const val = event.split(" ");
    setVisualizationIDAndName({ id: val[0], name: val[1] });
  };


  const openNotificationWithIcon = (type, message, error) => {
    notification[type]({message, error});
  };


  function hideNewForm() {
    setCurrentDestination({ name: "", type: "gsheets", options: { spreadsheet_id: "", sheet_name: "", row: 1, column: "A" } });
    setVisualizationIDAndName({ id: query.visualizations[0].id, name: query.visualizations[0].name });
    if (showForm) {
      setShowForm(false);
    } else {
      setEditForm(false);
    }
  }


  function validateNewDestination() {
    // eslint-disable-next-line no-restricted-syntax
    for (const sheet of allDestinations) {
      if (sheet.name === currentDestination.name) {
        return false;
      }
    }
    return true;
  }


  const handleNewFormSubmit = async (event) => {
    event.preventDefault();
    // eslint-disable-next-line radix
    currentDestination.options.row = parseInt(currentDestination.options.row);
    if (showForm) {
      if (validateNewDestination()) {
        const postNewDestination = {
          url: `/api/visualization/${visualizationIDAndName.id}/destination`,
          method: "POST",
          data: currentDestination,
        };
        await axios(postNewDestination)
          .then((res) => {
            if (res.status === 200) {
              openNotificationWithIcon("success", "Destination created", "");
              setModalType(String(res.data.destination.id));
            } else {
              openNotificationWithIcon("error", "Failed to create Destination", "");
            }
          })
          .catch((error) => {
            openNotificationWithIcon("error", error, "");
          });
      } else {
        openNotificationWithIcon("info", "Destination with same name already exists", "");
      }
      setShowForm(false);
    } else {
      const updateDestination = {
        url: `/api/visualization/${currentDestination.visualization_id}/destination/${currentDestination.id}`,
        method: "POST",
        data: currentDestination,
      };
      await axios(updateDestination)
        .then((res) => {
          if (res.status === 200) {
            openNotificationWithIcon("success", "Destination updated", "");
            setModalType([...modalType, "edited"]);
          } else {
            openNotificationWithIcon("error", "Failed to update Destination", "");
          }
        })
        .catch((error) => {
          openNotificationWithIcon("error", error, "");
        });
      setEditForm(false);
    }
    setCurrentDestination({ name: "", type: "gsheets", options: { spreadsheet_id: "", sheet_name: "", row: 1, column: "A" } });
    setVisualizationIDAndName({ id: query.visualizations[0].id, name: query.visualizations[0].name });
  };


  function showTutorial() {
    // TODO: Do not hard code this email and try to pass via parameters
    return (
      // eslint-disable-next-line react/jsx-no-target-blank
      <div className="m-b-20">To send data to a Google Sheet, please make sure you have editor access to the respective sheet and the sheet belong to the organization. Also, checkout how to add a destination <a href="https://data.grofer.io/how-to/redash/destinations/gsheets" target="_blank">here</a>.</div>
    );
  }


  const editDestination = (destination) => {
    setEditForm(true);
    setCurrentDestination(destination);
    // eslint-disable-next-line no-restricted-syntax
    for (const vis of query.visualizations) {
      if (vis.id === destination.visualization_id) {
        setVisualizationIDAndName({ id: vis.id, name: vis.name });
      }
    }
  };

  function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  const RqJobStatus = {
    QUEUE: 1,
    RUNNING: 2,
    SUCCEEDED: 3,
    FAILED: 4,
  };

  const handleRqJob = async (destinationId, jobId) => {
    const getRqJobStatus = {
      url: `/api/destination/${destinationId}/jobs/${jobId}`,
      method: "GET",
    };
    await axios(getRqJobStatus)
      .then(async (res) => {
        if (res.data.job.status === RqJobStatus.SUCCEEDED) {
          openNotificationWithIcon("success", "Destination synced", "");
          setModalType(String(jobId));
          setSyncDestination(Object.values(syncDestination).filter(item => item !== destinationId));
          setSyncDisabled(false);
        } else if (res.data.job.status === RqJobStatus.FAILED) {
          openNotificationWithIcon("error", "Failed to sync Destination", res.data.job.error);
          setModalType(String(jobId));
          setSyncDestination(Object.values(syncDestination).filter(item => item !== destinationId));
          setSyncDisabled(false);
        } else {
          await sleep(1000);
          await handleRqJob(destinationId, jobId);
        }
      })
      .catch((error) => {
        openNotificationWithIcon("error", error, "");
        setSyncDestination(Object.values(syncDestination).filter(item => item !== destinationId));
        setSyncDisabled(false);
      });
  };

  const handleSync = async (destinationId) => {
    setSyncDestination([...syncDestination, destinationId]);
    const postSyncDestination = {
      url: `/api/destination/${destinationId}/sync`,
      method: "POST",
    };
    let jobId = "";
    await axios(postSyncDestination)
      .then((res) => {
        jobId = res.data.job.id;
        setSyncDisabled(true);
      })
      .catch((error) => {
        openNotificationWithIcon("error", error, "");
        setSyncDestination(Object.values(syncDestination).filter(item => item !== destinationId));
      });
    await handleRqJob(destinationId, jobId);
  };


  const handleDeleteDestination = async (visualizationId, destinationId) => {
    const deleteDestination = {
      url: `/api/visualization/${visualizationId}/destination/${destinationId}`,
      method: "DELETE",
    };
    await axios(deleteDestination)
      .then((res) => {
        if (res.status === 200) {
          openNotificationWithIcon("success", "Destination deleted", "");
          setModalType([...modalType, "deleted"]);
        } else {
          openNotificationWithIcon("error", "Failed to delete Destination", "");
        }
      })
      .catch((error) => {
        openNotificationWithIcon("error", error, "");
      });
  };


  function showConfirm(visualizationId, destinationId) {
    confirm({
      title: "Do you want to delete this destination?",
      onOk() {
        handleDeleteDestination(visualizationId, destinationId);
      },
      onCancel() {
      },
    });
  }


  function showDestinationDetails() {
    if (allDestinations.length === 0) return <div className="list-group-item">There is no destination</div>;
    return (
      allDestinations.map(
        destination => (
          <div key={destination.id} className="list-group-item m-b-15 p-10">
            <p style={{ display: "inline-block", width: 180, margin: 0 }}>
              {destination.name.length > 20 ? destination.name.substr(0, 19) + "..." : destination.name}
            </p>
            <p style={{ display: "inline-block", width: 65, margin: 0 }}>Last sync: </p>
            <p style={{ display: "inline-block", width: 150, margin: 0, color: destination.last_sync && destination.last_sync.status === "finished" ? "green" : "red" }}>
              <Tooltip placement="bottom" title={destination.last_sync && destination.last_sync.error}>
                <span>{destination.last_sync ? formatDateTime(destination.last_sync.timestamp) : "Not Applicable"}</span>
              </Tooltip>
            </p>
            <p style={{ display: "inline-block", width: 50, margin: 0 }}>
              <a onClick={() => editDestination(destination)}>
                <i title="edit" className="fa fa-pencil-square-o fa-lg m-l-5 m-r-5" aria-hidden="true" />
              </a>
            </p>
            <p style={{ display: "inline-block", width: 55, margin: 0 }}>
              <a onClick={() => (syncDisabled ? openNotificationWithIcon("info", "Please wait...", "A destination sync is already running.") : handleSync(destination.id))}>
                <i title="sync" className={syncDestination.includes(destination.id) ? "fa fa-refresh fa-spin fa-lg m-l-5 m-r-5" : "fa fa-refresh fa-lg m-l-5 m-r-5"} aria-hidden="true" />
              </a>
            </p>
            <p style={{ display: "inline-block", margin: 0 }}>
              <a onClick={() => showConfirm(destination.visualization_id, destination.id)}>
                <i title="delete" style={{ color: "red" }} className="fa fa-trash-o fa-lg m-l-5 m-r-5" aria-hidden="true" />
              </a>
            </p>
          </div>
        ),
      )
    );
  }


  function showOrEdit() {
    if (showForm) {
      return (
        <>
          <div className="m-b-10" style={{ padding: 5, fontWeight: "bold" }}>Create New Destination</div>
          <div>
            <label htmlFor="select-visualization" style={{ padding: 5, width: 140 }}>Select Visualization</label>
            <Select style={{ width: 380 }} className="w-51" value={visualizationIDAndName.name} onChange={handleVisualizationID}>
              {
                query.visualizations.map(
                  vis => (vis.type === "TABLE" &&
                    <Select.Option key={String(vis.id) + " " + vis.name}>{vis.name}</Select.Option>
                  ),
                )
              }
            </Select>
          </div>
        </>
      );
    }
    return <div className="m-b-10" style={{ padding: 5, fontWeight: "bold" }}>Edit Destination</div>;
  }


  function showDestinationForm() {
    return (
      <form className="list-group-item" onSubmit={handleNewFormSubmit}>
        {showOrEdit()}
        <div style={{ paddingTop: 10 }} hidden={typeof currentDestination.id === "undefined"}>
          <label htmlFor="destination-id" style={{ padding: 5, width: 140 }}>Destination ID</label>
          <input className="ant-input" type="text" name="destination_id" value={currentDestination.id} disabled style={{ width: 380 }} />
        </div>
        <div style={{ paddingTop: 10 }}>
          <label htmlFor="destination-name" style={{ padding: 5, width: 140 }}>Destination Name</label>
          <input className="ant-input" type="text" name="sheet_id" placeholder="Assign a Name to your Destination" value={currentDestination.name} required style={{ width: 380 }} onChange={handleDestinationName} />
        </div>
        <div style={{ paddingTop: 10 }}>
          <label htmlFor="sheet-id" style={{ padding: 5, width: 140 }}>Sheet ID</label>
          <input className="ant-input" type="text" name="sheet_id" placeholder="Sheet Link where you want to import data" value={currentDestination.options.spreadsheet_id} required style={{ width: 380 }} onChange={handleSheetID} />
        </div>
        <div style={{ paddingTop: 10 }}>
          <label htmlFor="sheet-name" style={{ padding: 5, width: 140 }}>Sheet Name</label>
          <input className="ant-input" type="text" name="sheet_name" placeholder="Sheet Name by default it's Sheet1" value={currentDestination.options.sheet_name} required style={{ width: 380 }} onChange={handleSheetName} />
        </div>
        <div style={{ paddingTop: 10 }}>
          {/* eslint-disable-next-line react/no-unescaped-entities */}
          <label htmlFor="cell-row" style={{ padding: 5, width: 140 }}>Cell Row ( { ">=" } 1 )</label>
          <input className="ant-input" type="number" name="cell_row" min="1" max="1000" value={currentDestination.options.row} required style={{ width: 60 }} onChange={handleCellRow} />
        </div>
        <div style={{ paddingTop: 10 }}>
          <label htmlFor="cell-column" style={{ padding: 5, width: 140 }}>Cell Column</label>
          <input className="ant-input" type="text" name="cell_column" pattern="[a-zA-Z]+" minLength="1" maxLength="3" value={currentDestination.options.column} required style={{ width: 50 }} onChange={handleCellColumn} />
        </div>
        <div style={{ paddingTop: 20, float: "right" }}>
          <button type="submit" className="btn btn-primary m-10"><span>Save</span></button>
          <button type="button" className="btn btn-primary m-10" onClick={hideNewForm}><span>Cancel</span></button>
        </div>
      </form>
    );
  }


  useEffect(() => {
    setIsLoaded(false);
    axios.get(`/api/queries/${query.id}/destinations`)
      .then((res) => {
        setAllDestinations(res.data.destinations);
        setIsLoaded(true);
      })
      .catch((error) => {
        setErrorWhileLoading(true);
        openNotificationWithIcon("error", error, "");
      });
  }, [modalType, query.id]);


  return (
    <Modal
      {...dialog.props}
      wrapClassName="ant-modal-centered"
      title="Sheet Destinations"
      okText="Save"
      width={600}
      footer={!showForm && !editForm && isLoaded && <button type="button" className="btn btn-primary" onClick={() => setShowForm(true)}>Add Destination</button>}
      wrapProps={{ "data-test": "EditDestinationDialog" }}
    >
      <Grid.Row gutter={24}>
        <Grid.Col span={24} md={24}>
          <div> { showTutorial() } </div>
          <div> { !isLoaded && !errorWhileLoading && <div style={{ textAlign: "center" }}>Loading Destinations. Please wait...</div> } </div>
          <div> { errorWhileLoading && <div style={{ textAlign: "center" }}>Failed to load Destinations.</div> } </div>
          <div> { !editForm && !showForm && isLoaded && showDestinationDetails() } </div>
          <div> { (showForm || editForm) && showDestinationForm() } </div>
        </Grid.Col>
      </Grid.Row>
    </Modal>
  );
}


EditDestinationDialog.propTypes = {
  dialog: DialogPropType.isRequired,
  query: PropTypes.object.isRequired, // eslint-disable-line react/forbid-prop-types
};


export default wrapDialog(EditDestinationDialog);
