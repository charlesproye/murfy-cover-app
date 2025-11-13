import React from 'react';
import ReportFooter from './web-report/ReportFooter';
import {
  IconBulb,
  IconShieldCheck,
  IconBattery,
  IconRuler,
  IconFileCheck,
} from '@tabler/icons-react';
import { InfoVehicleResult } from '@/interfaces/dashboard/passport/infoVehicle';
// import RangeInfo from './web-report/RangeInfo';
// import WarrantyInfo from './web-report/WarrantyInfo';
import ChargingSummary from './web-report/charging/ChargingSummary';

interface SecondPageProps {
  reportData: InfoVehicleResult;
}

const SecondPage: React.FC<SecondPageProps> = ({ reportData }) => {
  return (
    <>
      {' '}
      <div className="flex flex-col rounded-lg overflow-hidden bg-gray-background">
        <div className="flex flex-col md:flex-row gap-6 p-6 mb-4 max-w-4xl w-full">
          <ChargingSummary reportData={reportData} />
        </div>

        <h2 className="text-xl font-bold mb-2 px-6">Detailed Information</h2>

        {/* Contenu principal - utilise un container scrollable si nécessaire */}
        <div className="flex flex-col items-center px-6 py-4 gap-4">
          {/* State of Health */}
          <div className="bg-white rounded-xl p-4 md:p-5 shadow-sm max-w-4xl w-full">
            <div className="flex items-center space-x-2 mb-3">
              <div className="w-6 h-6 rounded-full flex items-center justify-center">
                <IconBulb size={18} className="text-green-rapport" />
              </div>
              <h3 className="font-medium">State of Health (SoH)</h3>
            </div>

            <p className="text-sm mb-3">
              The SoH is calculated by Bib using driving data, meaning raw data
              transmitted by the vehicle's BMS and stored in the manufacturer's cloud.
              This value does not come from a physical reading of the onboard computer nor
              from a manufacturer's calculation.
            </p>

            <p className="text-sm">
              Bib provides an independent SoH, which does not imply any manufacturer
              liability. The SoH displayed by the manufacturer for this vehicle is (insert
              manufacturer SoH if available).
            </p>
          </div>

          {/* Bib Score */}
          <div className="bg-white rounded-xl p-4 md:p-5 shadow-sm max-w-4xl w-full">
            <div className="flex items-center space-x-2 mb-3">
              <div className="w-6 h-6 rounded-full flex items-center justify-center">
                <IconShieldCheck size={18} className="text-green-rapport" />
              </div>
              <h3 className="font-medium">Bib Score</h3>
            </div>

            <p className="text-sm mb-2">
              The <span className="font-bold">Bib Score</span> is based on several
              factors:
            </p>
            <ul className="list-disc pl-5 text-sm mb-3">
              <li>
                SoH, which reflects the battery's condition compared to its original
                state.
              </li>
              <li>
                A comparison of this vehicle's aging against the average aging of similar
                vehicles.
              </li>
            </ul>

            <p className="text-sm">
              For example, a vehicle with a "good" SoH (e.g., 85%) may still receive a
              "poor" Bib Score (e.g., D) if it falls significantly below the average for
              similar vehicles.
            </p>
          </div>

          {/* Vehicle & Battery Warranty */}
          <div className="bg-white rounded-xl p-4 md:p-5 shadow-sm max-w-4xl w-full">
            <div className="flex items-center space-x-2 mb-3">
              <div className="w-6 h-6 rounded-full flex items-center justify-center">
                <IconBattery size={18} className="text-green-rapport" />
              </div>
              <h3 className="font-medium">Vehicle & Battery Warranty</h3>
            </div>

            <p className="text-sm mb-2">
              EVs are covered by two different manufacturer warranties:
            </p>
            <ul className="list-disc pl-5 text-sm mb-3">
              <li>
                The vehicle warranty covers engine, powertrain, onboard electronics, and
                vehicle control systems, typically lasting 5 years.
              </li>
              <li>
                The battery warranty is related specifically to the electrical function
                and health of the battery. It guarantees a certain level of capacity and
                performance, usually more than 70% SoH. The standard model for the battery
                warranty is {reportData.vehicle_info?.warranty_date} years or{' '}
                {reportData.vehicle_info?.warranty_km} km, but it may vary by brand and
                manufacturer. More details can be found on the Bib battery warranty page.
              </li>
            </ul>
          </div>

          {/* Range Insights */}
          <div className="bg-white rounded-xl p-4 md:p-5 shadow-sm max-w-4xl w-full">
            <div className="flex items-center space-x-2 mb-3">
              <div className="w-6 h-6 rounded-full flex items-center justify-center">
                <IconRuler size={18} className="text-green-rapport" />
              </div>
              <h3 className="font-medium">Range Insights</h3>
            </div>

            <p className="text-sm">
              Bib compares the WLTP (Worldwide Harmonized Light Vehicle Test Procedure)
              range, as announced by the manufacturer at the time of the vehicle's
              production, with the current range observed during monitoring of the vehicle
              by Bib Batteries. This comparison helps assess how the vehicle's battery
              performance has changed over time and how it aligns with the manufacturer's
              initial estimates, providing a more accurate understanding of the vehicle's
              current energy efficiency.
            </p>
          </div>

          {/* Report Validity */}
          <div className="bg-white rounded-xl p-4 md:p-5 shadow-sm max-w-4xl w-full">
            <div className="flex items-center space-x-2 mb-3">
              <div className="w-6 h-6 rounded-full flex items-center justify-center">
                <IconFileCheck size={18} className="text-green-rapport" />
              </div>
              <h3 className="font-medium">Report Validity</h3>
            </div>

            <p className="text-sm">
              This Report verifies the performance and condition of the electric vehicle
              at the time of the testing based on the data provided during the monitoring
              of the vehicle. This Report is not a warranty or guarantee of the
              performance and condition of vehicle, and any modification made to the
              vehicle after the testing phase may invalidate this Report. Full terms and
              conditions can be found at www.bib-batteries.fr.
            </p>
          </div>
        </div>

        {/* Footer */}
        <div className="mt-auto">
          <ReportFooter />
        </div>
      </div>
    </>
  );
};

export default SecondPage;
