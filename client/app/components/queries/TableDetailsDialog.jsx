import axios from 'axios';
import React from 'react';
import Button from 'antd/lib/button';
import Modal from 'antd/lib/modal';
import moment from 'moment';
import PropTypes from 'prop-types';
import { wrap as wrapDialog, DialogPropType } from '@/components/DialogWrapper';
import './TableDetailsDialog.css';

function ErrorComponent() {
  return (
    <div className="error-component">
      <i className="zmdi zmdi-alert-circle-o" />
      <h4>
        <span>Some issue occurred while fetching details for this table. If the issue persists, please contact Data Platform Team</span>
      </h4>
    </div>
  );
}

function LoadComponent() {
  return (
    <div className="error-component">
      <h4>Fetching table details...</h4>
    </div>
  );
}

function DetailsComponent(props) {
  // eslint-disable-next-line react/prop-types
  const details = props.details;
  if (details.error) {
    return (
      <div>
        <div key="error" className="details-component">
          <p style={{ textAlign: 'center', paddingLeft: '15%' }}>{details.error}</p>
        </div>
      </div>
    );
  }
  return (
    <div>
      <div key="name" className="details-component">
        <h5>Name</h5>
        <p>{details.name}</p>
      </div>
      <div key="description" className="details-component">
        <h5>Description</h5>
        <p>{details.description || 'not available currently, please reach out to the owner'}</p>
      </div>
      <div key="owner" className="details-component">
        <h5>Owner</h5>
        <p>
          {
            details.owners.map((owner, i) => [
              i > 0 && ', ',
              <a key={owner} target="_blank" rel="noopener noreferrer" href={`mailto: ${owner}`}>{owner}</a>,
            ])
          }
        </p>
      </div>
      <div key="last_refresh" className="details-component">
        <h5>Last refresh</h5>
        <p>{moment(details.lastRefresh).format('YYYY-MM-DD hh:mm A')} IST ({moment(details.lastRefresh).fromNow()})</p>
      </div>
      <div key="partition_keys" className="details-component">
        <h5>Partition Key(s)</h5>
        <p>{details.partitionKeys || 'Not Applicable'}</p>
      </div>
      {
        details.properties && Object.keys(details.properties).length > 0 ?
          Object.keys(details.properties).map(key => (
            <div key={details.properties[key]} className="details-component">
              <h5>{key}</h5>
              <p>{details.properties[key]}</p>
            </div>
          )) : null
      }
      {
        details.docs && details.docs.length > 0 ? (
          <div key="docs" className="details-component">
            <h5>Related docs</h5>
            <p>
              {
                details.docs.map((doc, i) => [
                  i > 0 && ', ',
                  <a key={doc.url} target="_blank" rel="noopener noreferrer" href={doc.url}>{doc.description}</a>,
                ])
              }
            </p>
          </div>
        ) : null
      }
      <div key="catalog" className="catalog-link">
        <b><a target="_blank" rel="noopener noreferrer" href={details.catalog}>(View catalog)</a></b>
      </div>
    </div>
  );
}

class TableDetailsDialog extends React.Component {
  static propTypes = {
    catalog: PropTypes.string.isRequired,
    dialog: DialogPropType.isRequired,
  };

  state = {
    details: {},
    isLoading: true,
    error: false,
  };

  componentDidMount() {
    const catalog = this.props.catalog;
    const fetchDatasetConfig = {
      url: `/api/data_catalog/datahub?dataset_id=${catalog.split('/')[4]}`,
      method: 'GET',
    };

    axios(fetchDatasetConfig)
      .then((res) => {
        if (Object.keys(res.data).length !== 0) {
          const { name, description, owners, lastRefresh, partitionKeys, properties, docs, error } = res.data;

          this.setState({
            details: { name, description, owners, lastRefresh, partitionKeys, properties, docs, catalog, error }, isLoading: false,
          });
        } else {
          this.setState({ error: true, isLoading: false });
        }
      });
  }

  render() {
    const { dialog } = this.props;
    let display;
    if (this.state.error) {
      display = <ErrorComponent />;
    } else if (this.state.isLoading) {
      display = <LoadComponent />;
    } else {
      display = <DetailsComponent details={this.state.details} />;
    }
    return (
      <Modal {...dialog.props} className="details" title="Table Details" footer={(<Button onClick={dialog.dismiss}>Close</Button>)}>
        {display}
      </Modal>
    );
  }
}

export default wrapDialog(TableDetailsDialog);