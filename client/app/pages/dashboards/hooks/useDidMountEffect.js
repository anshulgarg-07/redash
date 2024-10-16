import { useEffect, useRef } from 'react';

/**
    This custom hook run in the same phase that componentDidUpdate does.
    It will not run on first render, i.e when we assign an initial state.
    It will only run after the dependency states finally starts to change.
**/
const useDidMountEffect = (func, deps) => {
    const didMount = useRef(false);

    useEffect(() => {
        if (didMount.current) func();
        else didMount.current = true;
    }, deps);
};

export default useDidMountEffect;