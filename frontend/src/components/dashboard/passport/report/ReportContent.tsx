import React from 'react';
import FirstPage from './FirstPage';
import SecondPage from './SecondPage';
import { VehicleData } from './types';

interface ReportContentProps {
  data: unknown;
  currentPage: 1 | 2;
}

const ReportContent: React.FC<ReportContentProps> = ({ data, currentPage }) => {
  const vehicleData = data as VehicleData;
  if (!vehicleData) {
    return <div>Loading...</div>;
  }

  // Assurez-vous que la donnée est au bon format (objet vs tableau)
  const reportDataFormatted = Array.isArray(vehicleData) ? vehicleData[0] : vehicleData;

  return (
    <div className="flex flex-col">
      {/* Afficher la page courante */}
      {currentPage === 1 ? (
        <FirstPage reportData={reportDataFormatted} />
      ) : (
        <SecondPage reportData={reportDataFormatted} />
      )}
    </div>
  );
};

export default ReportContent;
