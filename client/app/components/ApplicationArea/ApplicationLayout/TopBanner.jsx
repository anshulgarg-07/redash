import React from 'react';

const TopBanner = ({message}) => (
  <div style={{ background: "#E9F7F8", padding: "10px", textAlign: "center" }}>
    <span style={{ color: '#000', fontFamily: 'Verdana', fontWeight: '510' }}>
      {message}
    </span>
  </div>
);

export default TopBanner;