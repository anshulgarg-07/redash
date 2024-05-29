import React, { useState } from 'react';

const InfoIcon = ({ message }) => {
    const [showTooltip, setShowTooltip] = useState(false);

    const containerStyle = {
        position: 'relative',
        display: 'inline-block',
        cursor: 'pointer',
    };

    const iconStyle = {
        width: '15px',
        height: '15px',
        borderRadius: '50%',
        border: '2px solid #007BFF',
        color: '#007BFF',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        fontWeight: 'bold',
        fontFamily: 'Arial, sans-serif',
        fontSize: '10px',
        lineHeight: '20px',
        backgroundColor: 'transparent',
    };

    const tooltipStyle = {
        visibility: showTooltip ? 'visible' : 'hidden',
        width: '200px',
        backgroundColor: 'black',
        color: '#fff',
        textAlign: 'center',
        borderRadius: '5px',
        padding: '5px',
        position: 'absolute',
        zIndex: 1,
        bottom: '125%', // Position the tooltip above the info icon
        left: '50%',
        transform: 'translateX(-50%)', // Center the tooltip
        opacity: showTooltip ? 1 : 0,
        transition: 'opacity 0.3s',
    };

    return (
        <div
            style={containerStyle}
            onMouseEnter={() => setShowTooltip(true)}
            onMouseLeave={() => setShowTooltip(false)}
        >
            <div style={iconStyle}>i</div>
            <div style={tooltipStyle}>{message}</div>
        </div>
    );
};

export default InfoIcon;
