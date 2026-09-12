import { BrowserRouter, Route, Routes } from "react-router-dom";
import Overview from "./pages/Overview";
import ResourceDetail from "./pages/ResourceDetail";

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Overview />} />
        <Route path="/resources/:resourceId" element={<ResourceDetail />} />
      </Routes>
    </BrowserRouter>
  );
}
