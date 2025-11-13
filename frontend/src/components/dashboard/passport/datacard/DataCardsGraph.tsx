// This is not used anymore, if it has to be again,
// Rework the way we provide data to DataCard (and then ScoreCard)
// because it has changed since (ex in DataCardsHome)

// 'use client';

// import React, { useState } from 'react';
// import { PDFDownloadLink } from '@react-pdf/renderer';
// import { IconDownload, IconEye } from '@tabler/icons-react';

// import useGetDataCardGraph from '@/hooks/dashboard/passport/useGetDataCardGraph';
// import useGetDataPdf from '@/hooks/pdf/useGetDataPdf';
// import PassportPdf from '@/components/pdf/PassportPdf';
// import { Score } from '@/interfaces/dashboard/passport/ScoreCard/ScoreCardProps';
// import { formatNumber } from '@/utils/formatNumber';

// import DataGraphGetIcon from './DataGraphGetIcon';
// import DataCard from '@/components/common/DataCard';
// import ScoreCard from '@/components/dashboard/passport/datacard/ScoreCard';
// import BibRecommendation from '../report/BibRecommendation';
// import ReportModal from '../report/ReportModal';
// import useGetInfoVehicle from '@/hooks/dashboard/passport/useGetInfoVehicle'; // import useGetInfoVehicle from '@/hooks/dashboard/passport/useGetInfoVehicle';

// const DataCardsGraph: React.FC<{ vin: string | undefined }> = ({ vin }) => {
//   const { data: dataCard } = useGetDataCardGraph(vin);
//   const { data: dataPdf, isLoading: isLoadingPdf } = useGetDataPdf(vin);
//   const { data: infoVehicle, isLoading: isLoadingInfoVehicle } = useGetInfoVehicle(vin);

//   const isLoading = isLoadingPdf || isLoadingInfoVehicle;

//   const [showReportModal, setShowReportModal] = useState(false);

//   if (dataCard && dataCard.length !== 0) {
//     const cardConfigs = [
//       {
//         title: 'SoH (%)',
//         icon: 'soh',
//         type: 'default',
//         format: (value: number) => value.toString(),
//       },
//       {
//         title: 'Mileage (km)',
//         icon: 'odometer',
//         type: 'default',
//         format: (value: number) => formatNumber(value),
//       },
//       { title: 'Score', icon: 'score', type: 'score' },
//     ];

//     const valueTab: [(number | Score)[], (number | Score)[], (number | Score)[]] = [
//       [...dataCard[2].data].reverse(),
//       [...dataCard[0].data].reverse(),
//       [...dataCard[1].data].reverse(),
//     ];

//     const viewReport = (): void => {
//       setShowReportModal(true);
//     };

//     return (
//       <div className="flex flex-col justify-around h-full w-full pt-2">
//         <div className="flex flex-col content-center mobile:flex-row mobile:content-start flex-wrap justify-center gap-2 mac:justify-between">
//           {valueTab.map((tab, index) =>
//             cardConfigs[index].type === 'score' ? (
//               <ScoreCard
//                 title={cardConfigs[index].title}
//                 getIcon={<DataGraphGetIcon icon={cardConfigs[index].icon} />}
//                 statTab={tab as number[]}
//                 key={index}
//                 score={tab[tab.length - 1] as Score}
//               />
//             ) : (
//               <DataCard
//                 title={cardConfigs[index].title}
//                 getIcon={<DataGraphGetIcon icon={cardConfigs[index].icon} />}
//                 statTab={tab.map((value) =>
//                   typeof value === 'number' && cardConfigs[index].format
//                     ? cardConfigs[index].format(value)
//                     : value.toString(),
//                 )}
//                 key={index}
//               />
//             ),
//           )}

//           <div className="flex flex-col items-center -mt-[32px]">
//             <div className="flex gap-2 mb-2">
//               <div
//                 onClick={viewReport}
//                 className={`bg-white shadow-md rounded-full px-3 py-1 text-xs flex items-center gap-1 transition-colors ${
//                   isLoading ? 'opacity-50 cursor-not-allowed' : 'cursor-pointer'
//                 }`}
//               >
//                 <span className="text-primary">See Report</span>
//                 <IconEye className="text-primary w-4 h-4" />
//               </div>

//               {!isLoading && dataPdf && (
//                 <PDFDownloadLink
//                   document={<PassportPdf data={infoVehicle} />}
//                   fileName="Battery_Report.pdf"
//                 >
//                   {({ loading }) => (
//                     <div
//                       className={`bg-white shadow-md rounded-full px-3 py-1 text-xs flex items-center gap-1 transition-colors ${
//                         loading ? 'opacity-50 cursor-not-allowed' : 'cursor-pointer'
//                       }`}
//                     >
//                       <span className="text-primary">Download PDF</span>
//                       <IconDownload
//                         className={`text-primary w-4 h-4 ${loading ? 'animate-spin' : ''}`}
//                       />
//                     </div>
//                   )}
//                 </PDFDownloadLink>
//               )}
//             </div>

//             <BibRecommendation score={valueTab[2][valueTab[2].length - 1] as Score} />
//           </div>
//         </div>

//         {showReportModal && infoVehicle && (
//           <ReportModal onClose={() => setShowReportModal(false)} data={infoVehicle} />
//         )}
//       </div>
//     );
//   }

//   return null;
// };

// export default DataCardsGraph;
