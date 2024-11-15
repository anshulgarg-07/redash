import React, { useRef, useCallback, useState } from "react";
import PropTypes from "prop-types";
import DynamicComponent from "@/components/DynamicComponent";
import DesktopNavbar from "./DesktopNavbar";
import MobileNavbar from "./MobileNavbar";
import TopBanner from "./TopBanner";

import "./index.less";
import { isEmpty } from "lodash";

export const ApplicationLayoutContext = React.createContext(null);

export default function ApplicationLayout({ children }) {
  const mobileNavbarContainerRef = useRef();
  const [bannerText, setBannerText] = useState();

  const getMobileNavbarPopupContainer = useCallback(() => mobileNavbarContainerRef.current, []);
  return (
    <ApplicationLayoutContext.Provider value={{ bannerText, setBannerText }}>
      <React.Fragment>
        <DynamicComponent name="ApplicationWrapper">
          <div className="application-layout-side-menu">
            <DynamicComponent name="ApplicationDesktopNavbar">
              <DesktopNavbar />
            </DynamicComponent>
          </div>
          <div className="application-layout-content">
            {!isEmpty(bannerText) && (
              <TopBanner message={bannerText} />
            )}
            <nav className="application-layout-top-menu" ref={mobileNavbarContainerRef}>
              <DynamicComponent name="ApplicationMobileNavbar" getPopupContainer={getMobileNavbarPopupContainer}>
                <MobileNavbar getPopupContainer={getMobileNavbarPopupContainer} />
              </DynamicComponent>
            </nav>
            {children}
          </div>
        </DynamicComponent>
      </React.Fragment>
    </ApplicationLayoutContext.Provider>
  );
}

ApplicationLayout.propTypes = {
  children: PropTypes.node,
};

ApplicationLayout.defaultProps = {
  children: null,
};
