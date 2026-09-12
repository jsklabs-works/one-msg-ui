import { BrowserRouter, Route, Routes } from "react-router-dom";
import Layout from "./components/Layout";
import { MonitoringDataProvider } from "./context/MonitoringDataContext";
import BrokerView from "./pages/BrokerView";
import Dashboard from "./pages/Dashboard";
import ResourceDetail from "./pages/ResourceDetail";

export default function App() {
  return (
    <MonitoringDataProvider>
      <BrowserRouter>
        <Routes>
          <Route element={<Layout />}>
            <Route path="/" element={<Dashboard />} />
            <Route path="/brokers/:brokerId" element={<BrokerView />} />
            <Route path="/resources/:resourceId" element={<ResourceDetail />} />
          </Route>
        </Routes>
      </BrowserRouter>
    </MonitoringDataProvider>
  );
}
