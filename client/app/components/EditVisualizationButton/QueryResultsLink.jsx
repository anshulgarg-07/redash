import React from "react";
import PropTypes from "prop-types";
import Link from "@/components/Link";

export default function QueryResultsLink(props) {
  let href = "";

  const { query, queryResult, fileType } = props;
  const resultId = queryResult.getId && queryResult.getId();
  const resultData = queryResult.getData && queryResult.getData();
  const salt = (new Date()).getTime();

  if (resultId && resultData && query.name) {
    if (query.id) {
      href = `api/queries/${query.id}/results/${resultId}.${fileType}${props.embed ? `?api_key=${props.apiKey}&${salt}` : ""}`;
    } else {
      href = `api/query_results/${resultId}.${fileType}?${salt}`;
    }
  }

  return (
    <Link target="_blank" rel="noopener noreferrer" disabled={props.disabled} href={href} download>
      {props.children}
    </Link>
  );
}

QueryResultsLink.propTypes = {
  query: PropTypes.object.isRequired, // eslint-disable-line react/forbid-prop-types
  queryResult: PropTypes.object, // eslint-disable-line react/forbid-prop-types
  fileType: PropTypes.string,
  disabled: PropTypes.bool.isRequired,
  embed: PropTypes.bool,
  apiKey: PropTypes.string,
  children: PropTypes.oneOfType([PropTypes.arrayOf(PropTypes.node), PropTypes.node]).isRequired,
};

QueryResultsLink.defaultProps = {
  queryResult: {},
  fileType: "csv",
  embed: false,
  apiKey: "",
};
