import { useCallback } from "react";
import EditDestinationDialog from "../EditDestinationDialog";
import recordEvent from "@/services/recordEvent";

export default function useOpenDestinationsEditor(query, onChange) {
  return useCallback(() => {
    recordEvent("destinations_opened", "query", query.id);
    EditDestinationDialog.showModal({
      query: query,
    });
  }, [query]);
}
