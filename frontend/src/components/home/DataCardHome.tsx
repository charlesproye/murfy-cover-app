import useGetKpisHome from '@/hooks/dashboard/home/useGetKpisHome';
import { DataCardResult } from '@/interfaces/common/DataCard';
import DataCard from '../common/DataCard';
import React from 'react';
import DataCardGetIcon from '../dashboard/global/datacard/DataCardGetIcon';
import { formatNumber, getPercentageVariation, NO_DATA } from '@/utils/formatNumber';
import { Fleet } from '@/interfaces/auth/auth';
import { IconArrowRight } from '@tabler/icons-react';
import Link from 'next/link';
import { Loading } from '../common/loading/loading';

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
        : NO_DATA,
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
      data.length > 0 ? formatNumber(data[0].vehicle_count) : NO_DATA,
    formatVariation: (data: DataCardResult[]) => {
      let difference = null;

      if (data.length >= 2) {
        const last = data[0].vehicle_count;
        const secondLast = data[1].vehicle_count;

        difference = last - secondLast;
      }
      return difference;
    },
    isScalarVariation: true,
  },
  {
    title: 'Favorites vehicles',
    icon: 'f_count',
    formatData: (data: DataCardResult[]) =>
      data.length > 0 ? formatNumber(data[0].pinned_count) : NO_DATA,
    formatVariation: () => null,
    actionElement: (
      <Link
        href="/dashboard/favorites"
        className="bg-primary rounded-full p-1 text-white"
      >
        <IconArrowRight />
      </Link>
    ),
  },
];

const DataCardsHome: React.FC<{ fleet: Fleet | null }> = ({ fleet }) => {
  const { data } = useGetKpisHome(fleet);

  if (!data || data.length === 0) {
    return <Loading />;
  }

  return (
    <>
      {cardConfigs.map((card, index) => (
        <DataCard
          key={`${card.title}-${index}`}
          title={card.title}
          getIcon={<DataCardGetIcon icon={card.icon} />}
          formattedData={card.formatData(data)}
          variation={card.formatVariation(data)}
          isScalarVariation={card.isScalarVariation}
          actionElement={card.actionElement}
        />
      ))}
    </>
  );
};

export default DataCardsHome;
