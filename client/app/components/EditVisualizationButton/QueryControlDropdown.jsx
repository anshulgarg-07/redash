import React, { useState } from "react";
import PropTypes from "prop-types";
import Dropdown from "antd/lib/dropdown";
import Menu from "antd/lib/menu";
import Button from "antd/lib/button";
import PlainButton from "@/components/PlainButton";
import { clientConfig } from "@/services/auth";

import PlusCircleFilledIcon from "@ant-design/icons/PlusCircleFilled";
import ShareAltOutlinedIcon from "@ant-design/icons/ShareAltOutlined";
import FileOutlinedIcon from "@ant-design/icons/FileOutlined";
import FileExcelOutlinedIcon from "@ant-design/icons/FileExcelOutlined";
import EllipsisOutlinedIcon from "@ant-design/icons/EllipsisOutlined";

import QueryResultsLink from "./QueryResultsLink";
import ExportModal from "@/components/ExportModal";
import { GoogleOutlined } from "@ant-design/icons";

export default function QueryControlDropdown(props) {

  const [modalVisible, setModalVisible] = useState()
  const [loading, setLoading] = useState()
  const [errorMessage, setErrorMessage] = useState()
  const [message, setMessage] = useState()

  let href = "";

  const resultId = props.queryResult.getId && props.queryResult.getId();
  const resultData = props.queryResult.getData && props.queryResult.getData();
  const salt = (new Date()).getTime();

  if (resultId && resultData && props.query.name) {
    if (props.query.id) {
      href = `api/queries/${props.query.id}/results/${resultId}.${"gsheets-export"}${props.embed ? `?api_key=${props.apiKey}&${salt}` : ""}`;
    } else {
      href = `api/query_results/${resultId}.${"gsheets-export"}?${salt}`;
    }
  }

  const exportToGsheets = () => {
    setModalVisible(true)
    setLoading(true)
    setErrorMessage(null)
    setMessage(null)

    const queryResultsFetchLink = href

    fetch(queryResultsFetchLink)
      .then((response) => response.json())
      .then((result) => {
        if (result.error) {
          throw new Error(result.error);
        }
        if (result.sheet_link) {
          return result.sheet_link;
        } else {
          throw new Error("Unexpected response format. No sheet_link found.");
        }
      })
      .then((msg) => {
        setLoading(false)
        setMessage(msg)
      })
      .catch((error) => {
        setLoading(false)
        setErrorMessage(error.message)
      });
  }

  const menu = (
    <Menu>
      {!props.query.isNew() && (!props.query.is_draft || !props.query.is_archived) && (
        <Menu.Item>
          <PlainButton onClick={() => props.openAddToDashboardForm(props.selectedTab)}>
            <PlusCircleFilledIcon /> Add to Dashboard
          </PlainButton>
        </Menu.Item>
      )}
      {!clientConfig.disablePublicUrls && !props.query.isNew() && (
        <Menu.Item>
          <PlainButton
            onClick={() => props.showEmbedDialog(props.query, props.selectedTab)}
            data-test="ShowEmbedDialogButton">
            <ShareAltOutlinedIcon /> Embed Elsewhere
          </PlainButton>
        </Menu.Item>
      )}
      <Menu.Item>
        <QueryResultsLink
          fileType="csv"
          disabled={props.queryExecuting || !props.queryResult.getData || !props.queryResult.getData()}
          query={props.query}
          queryResult={props.queryResult}
          embed={props.embed}
          apiKey={props.apiKey}>
          <FileOutlinedIcon /> Download as CSV File
        </QueryResultsLink>
      </Menu.Item>
      <Menu.Item>
        <QueryResultsLink
          fileType="tsv"
          disabled={props.queryExecuting || !props.queryResult.getData || !props.queryResult.getData()}
          query={props.query}
          queryResult={props.queryResult}
          embed={props.embed}
          apiKey={props.apiKey}>
          <FileOutlinedIcon /> Download as TSV File
        </QueryResultsLink>
      </Menu.Item>
      <Menu.Item>
        <QueryResultsLink
          fileType="xlsx"
          disabled={props.queryExecuting || !props.queryResult.getData || !props.queryResult.getData()}
          query={props.query}
          queryResult={props.queryResult}
          embed={props.embed}
          apiKey={props.apiKey}>
          <FileExcelOutlinedIcon /> Download as Excel File
        </QueryResultsLink>
      </Menu.Item>
      <Menu.Item onClick={exportToGsheets}>
        <GoogleOutlined/> Export to Google Sheets
      </Menu.Item>
    </Menu>
  );

  return (
    <>
      <Dropdown trigger={["click"]} overlay={menu} overlayClassName="query-control-dropdown-overlay">
        <Button data-test="QueryControlDropdownButton">
          <EllipsisOutlinedIcon rotate={90} />
        </Button>
      </Dropdown>
      <ExportModal
        onCancel={() => setModalVisible(false)}
        visible={modalVisible}
        sheetLink={message}
        isLoading={loading}
        errorMessage={errorMessage}/>
    </>
  );
}

QueryControlDropdown.propTypes = {
  query: PropTypes.object.isRequired, // eslint-disable-line react/forbid-prop-types
  queryResult: PropTypes.object, // eslint-disable-line react/forbid-prop-types
  queryExecuting: PropTypes.bool.isRequired,
  showEmbedDialog: PropTypes.func.isRequired,
  embed: PropTypes.bool,
  apiKey: PropTypes.string,
  selectedTab: PropTypes.oneOfType([PropTypes.string, PropTypes.number]),
  openAddToDashboardForm: PropTypes.func.isRequired,
};

QueryControlDropdown.defaultProps = {
  queryResult: {},
  embed: false,
  apiKey: "",
  selectedTab: "",
};
