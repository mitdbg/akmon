// Vite's `?url` import: the asset's served URL as a string. Declared here
// rather than by `vite/client` so the operator UI's type surface does not
// gain every Vite global for one import (the PDF worker's).
declare module "*?url" {
  const url: string;
  export default url;
}
