import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import Layout from './components/Layout'
import DataSources from './pages/DataSources'
import DataQuality from './pages/DataQuality'
import PipelineBuilder from './pages/PipelineBuilder'
import DataModeling from './pages/DataModeling'
import Orchestration from './pages/Orchestration'
import StorageArchitecture from './pages/StorageArchitecture'
import Monitoring from './pages/Monitoring'
import DataCatalog from './pages/DataCatalog'
import Governance from './pages/Governance'
import Reporting from './pages/Reporting'
import KnowledgeBase from './pages/KnowledgeBase'
import BigData from './pages/BigData'
import CICD from './pages/CICD'
import DataContracts from './pages/DataContracts'
import CDC from './pages/CDC'
import ReverseETL from './pages/ReverseETL'
import FeatureStore from './pages/FeatureStore'
import CloudServices from './pages/CloudServices'
import IaC from './pages/IaC'
import Incidents from './pages/Incidents'
import DataMesh from './pages/DataMesh'
import RealTimeOLAP from './pages/RealTimeOLAP'
import NoSQL from './pages/NoSQL'
import DataVersioning from './pages/DataVersioning'
import CapacityPlanning from './pages/CapacityPlanning'
import Leadership from './pages/Leadership'

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Layout />}>
          <Route index element={<Navigate to="/sources" replace />} />
          <Route path="sources" element={<DataSources />} />
          <Route path="quality" element={<DataQuality />} />
          <Route path="pipeline" element={<PipelineBuilder />} />
          <Route path="modeling" element={<DataModeling />} />
          <Route path="orchestration" element={<Orchestration />} />
          <Route path="storage" element={<StorageArchitecture />} />
          <Route path="monitoring" element={<Monitoring />} />
          <Route path="catalog" element={<DataCatalog />} />
          <Route path="governance" element={<Governance />} />
          <Route path="reporting" element={<Reporting />} />
          <Route path="learn" element={<KnowledgeBase />} />
          <Route path="bigdata" element={<BigData />} />
          <Route path="cicd" element={<CICD />} />
          <Route path="contracts" element={<DataContracts />} />
          <Route path="cdc" element={<CDC />} />
          <Route path="reversetl" element={<ReverseETL />} />
          <Route path="featurestore" element={<FeatureStore />} />
          <Route path="cloud" element={<CloudServices />} />
          <Route path="iac" element={<IaC />} />
          <Route path="incidents" element={<Incidents />} />
          <Route path="datamesh" element={<DataMesh />} />
          <Route path="olap" element={<RealTimeOLAP />} />
          <Route path="nosql" element={<NoSQL />} />
          <Route path="versioning" element={<DataVersioning />} />
          <Route path="capacity" element={<CapacityPlanning />} />
          <Route path="leadership" element={<Leadership />} />
        </Route>
      </Routes>
    </BrowserRouter>
  )
}
