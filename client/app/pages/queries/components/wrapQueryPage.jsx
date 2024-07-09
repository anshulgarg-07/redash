import React, { useState, useEffect, useContext } from "react";
import PropTypes from "prop-types";
import LoadingState from "@/components/items-list/components/LoadingState";
import { Query } from "@/services/query";
import useImmutableCallback from "@/lib/hooks/useImmutableCallback";

import { ApplicationLayoutContext } from "@/components/ApplicationArea/ApplicationLayout";

export default function wrapQueryPage(WrappedComponent) {
  function QueryPageWrapper({ queryId, onError, ...props }) {
    const [query, setQuery] = useState(null);
    const { setBannerText } = useContext(ApplicationLayoutContext)

    const handleError = useImmutableCallback(onError);

    useEffect(() => {
      let isCancelled = false;
      const promise = queryId ? Query.get({ id: queryId }) : Promise.resolve(Query.newQuery());
      promise
        .then(result => {
          if (!isCancelled) {
            setQuery(result);
          }
          setBannerText(result.banner_text)
        })
        .catch(handleError);

      return () => {
        isCancelled = true;
      };
    }, [queryId, handleError, setBannerText]);

    if (!query) {
      return <LoadingState className="flex-fill" />;
    }

    return <WrappedComponent query={query} onError={onError} {...props} />;
  }

  QueryPageWrapper.propTypes = {
    queryId: PropTypes.oneOfType([PropTypes.number, PropTypes.string]),
  };

  QueryPageWrapper.defaultProps = {
    queryId: null,
  };

  return QueryPageWrapper;
}
