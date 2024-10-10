import React from "react";
import Modal from "antd/lib/modal";
import Spin from "antd/lib/spin";
import Alert from "antd/lib/alert";
import { LoadingOutlined } from "@ant-design/icons";

const ExportModal = ({ visible, onCancel, sheetLink, isLoading, errorMessage }) => {
  const antIcon = <LoadingOutlined style={{ fontSize: 48, color: "#52c41a" }} spin />;
  return (
    <Modal
          title={<span style={{ fontWeight: "bold", fontSize: "18px" }}>Exporting data to Google Sheet</span>}
          visible={visible}
          footer={null}
          onCancel={onCancel}
        >
          {isLoading ? (
            <div style={{ textAlign: "center", padding: "20px 0" }}>
               <Spin indicator={antIcon} />
              <p style={{ marginTop: "15px", fontSize: "16px", color: "#1890ff" }}>Preparing your sheet...</p>
            </div>
          ) : errorMessage ? (
            <div style={{ textAlign: "center", padding: "20px 0" }}>
              <Alert
                message="Error"
                description={errorMessage}
                type="error"
                showIcon
                style={{ marginBottom: "15px", fontSize: "16px" }}
              />
            </div>
          ) : (
            <div style={{ textAlign: "center", padding: "20px 0" }}>
              <div style={{ fontSize: "24px", color: "#52c41a", marginBottom: "10px" }}>
                <i className="zmdi zmdi-check-circle" aria-hidden="true" />
              </div>
              <p style={{ fontSize: "18px", fontWeight: "bold", color: "#52c41a" }}>
                Your data has been exported successfully!
              </p>
              <p style={{ fontSize: "16px", marginTop: "10px" }}>
                <span>Access the sheet here: </span>
                <a
                  href={sheetLink}
                  target="_blank"
                  rel="noopener noreferrer"
                  style={{
                    fontSize: "16px",
                    color: "#1890ff",
                    textDecoration: "underline",
                    fontWeight: "bold",
                  }}
                >
                  Open Google Sheet
                </a>
              </p>
            </div>
          )}
        </Modal>
  );
};

export default ExportModal