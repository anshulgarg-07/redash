document.addEventListener("DOMContentLoaded", async function() {

    async function checkChrome() {
    if (navigator.userAgentData && navigator.userAgentData.brands) {
      const brands = navigator.userAgentData.brands;
      for (const brand of brands) {
        if (brand.brand === 'Google Chrome') {
          return true;
        }
      }
    }

    return false;
    }

    async function checkIsMobile() {
    let platform;
    if (navigator.userAgentData && navigator.userAgentData.platform) {
      const platform = navigator.userAgentData.platform;
    } else {
      const platform = 'android'
    }
    return /android|iPad|iPhone|iPod|windows phone/i.test(platform);
    }

    async function detectBrowser() {
        return new Promise((resolve) => {
            let browserName = "Unknown";

            if (navigator.brave) {
                navigator.brave.isBrave().then(isBrave => {
                    if (isBrave) {
                        browserName = "Brave";
                    }
                    resolve(browserName);
                });
            } else if (navigator.vendor.includes("Apple")) {
                browserName = "Safari";
                resolve(browserName);
            } else if (!!window.chrome && !navigator.userAgent.includes("Edg")) {
                browserName = "Chrome";
                resolve(browserName);
            } else if (typeof InstallTrigger !== 'undefined') {
                browserName = "Firefox";
                resolve(browserName);
            } else if (navigator.userAgent.includes("Edg")) {
                browserName = "Edge";
                resolve(browserName);
            } else {
                resolve(browserName);
            }
        });
    }

    // const browserName = await detectBrowser();
    // const isChromeOrBrave = (browserName === "Brave" || browserName === "Chrome");
    //  const isChrome = (browserName === "Chrome");
    const isChrome = await checkChrome();
    const isMobile = await checkIsMobile();
    // Display the Google login button if the browser is Chrome or Brave
    if (!isChrome || isMobile) {
        document.body.innerHTML = `Please use Google Chrome on a desktop to access Redash.`;
        // document.body.innerHTML = `You are using ${browserName} browser. To access Redash, please use Google Chrome.`;
    }
});