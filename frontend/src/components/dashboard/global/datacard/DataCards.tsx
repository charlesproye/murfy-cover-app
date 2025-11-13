import useGetDataCard from '@/hooks/dashboard/global/useGetDataCard';
import { DataCardResult } from '@/interfaces/common/DataCard';
import DataCard from '../../../common/DataCard';
import React from 'react';
import { SelectedFilter } from '@/interfaces/common/optionSelector/DataFilter';
import DataCardGetIcon from './DataCardGetIcon';
import { formatNumber, getPercentageVariation } from '@/utils/formatNumber';

const cardConfigs = [
  {
    title: 'Avg. Mileage (km)',
    icon: 'odometer',
    formatData: (data: DataCardResult[]) =>
      formatNumber(data.length > 0 ? data[0].avg_odometer : null),
    formatVariation: (data: DataCardResult[]) => {
      const cleanData = data
        .filter((val) => val.avg_odometer !== null && val.avg_odometer !== 0)
        .map((val) => val.avg_odometer as number);

      return getPercentageVariation(cleanData);
    },
  },
  {
    title: 'Avg. SoH (%)',
    icon: 'soh',
    formatData: (data: DataCardResult[]) =>
      data.length > 0 && typeof data[0].avg_soh === 'number'
        ? data[0].avg_soh.toFixed(1)
        : 'Unknown',
    formatVariation: (data: DataCardResult[]) => {
      const cleanData = data
        .filter((val) => val.avg_soh !== null && val.avg_soh !== 0)
        .map((val) => val.avg_soh as number);

      return getPercentageVariation(cleanData);
    },
  },
  {
    title: 'Fleet size',
    icon: 'vehicle',
    formatData: (data: DataCardResult[]) =>
      formatNumber(data.length > 0 ? data[0].vehicle_count : null),
    formatVariation: (data: DataCardResult[]) => {
      const cleanData = data
        .filter((val) => val.vehicle_count !== 0)
        .map((val) => val.vehicle_count);

      return getPercentageVariation(cleanData);
    },
  },
];

const DataCards: React.FC<{ filters: SelectedFilter[] }> = ({ filters }) => {
  const { data } = useGetDataCard(filters);

  if (!data || data.length === 0) {
    return <></>;
  }

  return (
    <>
      {cardConfigs.map((card, index) => (
        <DataCard
          key={index}
          title={card.title}
          getIcon={<DataCardGetIcon icon={card.icon} />}
          formattedData={card.formatData(data)}
          variation={card.formatVariation(data)}
        />
      ))}
    </>
  );
};

export default DataCards;
