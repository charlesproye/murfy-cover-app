import {
  DataCardFuturePdfProps,
  DataCardPdfProps,
  DataPdfResult,
  DataPurcentageBarPdfProps,
} from '@/interfaces/pdf/DataPdf';

const formatNumber = (value: string | number): string => {
  const num = typeof value === 'string' ? parseInt(value.replace(/\s/g, '')) : value;
  return num.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ' ');
};

export const createDataCardsFuturMid = (
  data: DataPdfResult,
): DataCardFuturePdfProps[] => [
  {
    icon: [
      'M16 2a1 1 0 0 1 .993 .883l.007 .117v1h1a3 3 0 0 1 2.995 2.824l.005 .176v12a3 3 0 0 1 -2.824 2.995l-.176 .005h-12a3 3 0 0 1 -2.995 -2.824l-.005 -.176v-12a3 3 0 0 1 2.824 -2.995l.176 -.005h1v-1a1 1 0 0 1 1.993 -.117l.007 .117v1h6v-1a1 1 0 0 1 1 -1m3 7h-14v9.625c0 .705 .386 1.286 .883 1.366l.117 .009h12c.513 0 .936 -.53 .993 -1.215l.007 -.16z',
      'M8 14h2v2h-2z',
    ],
    title: `${data?.warranty_date ? data.warranty_date : 0} Years`,
    date: `${new Date().getDate()}-${new Date().getMonth() + 1}-${new Date().getFullYear() + data?.warranty_date}`,
    text: 'Remaining battery\n warranty',
  },
  {
    icon: [
      'M7 17m-2 0a2 2 0 1 0 4 0a2 2 0 1 0 -4 0',
      'M17 17m-2 0a2 2 0 1 0 4 0a2 2 0 1 0 -4 0',
      'M5 17h-2v-6l2 -5h9l4 5h1a2 2 0 0 1 2 2v4h-2',
      'm-4 0h-6',
      'm-6 -6h15',
      'm-6 0v-5',
    ],
    title: `${data?.remaining_warranty_km} km`,
    date: ``,
    text: 'Remaining battery\n warranty',
  },
];

export const createDataCardsMid = (data: DataPdfResult): DataCardPdfProps[] => [
  {
    icon: 'M7 17m-2 0a2 2 0 1 0 4 0a2 2 0 1 0 -4 0M17 17m-2 0a2 2 0 1 0 4 0a2 2 0 1 0 -4 0M5 17h-2v-6l2 -5h9l4 5h1a2 2 0 0 1 2 2v4h-2m-4 0h-6m-6 -6h15m-6 0v-5',
    title: `${formatNumber(data?.odometer)} km`,
    text: 'Mileage',
  },
  {
    icon: 'M7 10h14l-4 -4M17 14h-14l4 4',
    title: `${data?.consumption ? `${data?.consumption} kWh` : 'Unkwnown'}`,
    text: 'Average consumption\n(/100km)',
  },
  {
    icon: 'M13 2l.018 .001l.016 .001l.083 .005l.011 .002h.011l.038 .009l.052 .008l.016 .006l.011 .001l.029 .011l.052 .014l.019 .009l.015 .004l.028 .014l.04 .017l.021 .012l.022 .01l.023 .015l.031 .017l.034 .024l.018 .011l.013 .012l.024 .017l.038 .034l.022 .017l.008 .01l.014 .012l.036 .041l.026 .027l.006 .009c.12 .147 .196 .322 .218 .513l.001 .012l.002 .041l.004 .064v6h5a1 1 0 0 1 .868 1.497l-.06 .091l-8 11c-.568 .783 -1.808 .38 -1.808 -.588v-6h-5a1 1 0 0 1 -.868 -1.497l.06 -.091l8 -11l.01 -.013l.018 -.024l.033 -.038l.018 -.022l.009 -.008l.013 -.014l.04 -.036l.028 -.026l.008 -.006a1 1 0 0 1 .402 -.199l.011 -.001l.027 -.005l.074 -.013l.011 -.001l.041 -.002z',
    title: `${data?.cycles || 'Unkwnown'}`,
    text: 'Charging cycles',
  },
  {
    icon: 'M7 18v-11a2 2 0 0 1 2 -2h.5a.5 .5 0 0 0 .5 -.5a.5 .5 0 0 1 .5 -.5h3a.5 .5 0 0 1 .5 .5a.5 .5 0 0 0 .5 .5h.5a2 2 0 0 1 2 2v11a2 2 0 0 1 -2 2h-6a2 2 0 0 1 -2 -2',
    title: `${Math.round(data?.capacity * (data?.soh / 100))} kWh`,
    text: 'Remaining \n Battery Capacity',
  },
];

export const createDataPurcentageBar = (
  data: DataPdfResult,
): DataPurcentageBarPdfProps[] => [
  {
    percentage: Math.max(0, data?.predictions?.initial?.soh || 0),
    km: Math.max(0, data?.predictions?.initial?.range_max || 0),
    description: 'Initial range (WLTP)',
  },
  {
    percentage: Math.max(0, data?.predictions?.current?.soh || 0),
    km: Math.max(0, data?.predictions?.current?.range_max || 0),
    description: 'Current remaining range',
  },
  {
    percentage: Math.max(0, data?.predictions?.predictions?.[0]?.soh || 0),
    km: Math.max(0, data?.predictions?.predictions?.[0]?.range_max || 0),
    description: 'End of contract (km)',
  },
  {
    percentage: Math.max(0, data?.predictions?.predictions?.[1]?.soh || 0),
    km: Math.max(0, data?.predictions?.predictions?.[1]?.range_max || 0),
    description: 'End of warranty (km)',
  },
];
