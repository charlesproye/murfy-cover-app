import React from 'react';
import { IconClock } from '@tabler/icons-react';
import useGetInfoVehicle from '@/hooks/dashboard/passport/useGetInfoVehicle';

interface LastUpdatePassportProps {
  vin: string | undefined;
}

const LastUpdatePassport: React.FC<LastUpdatePassportProps> = ({ vin }) => {
  const { data, isLoading } = useGetInfoVehicle(vin);
  const lastDate = data?.last_data_date;

  const formatDate = (date: string): string => {
    const d = new Date(date);
    return d.toLocaleDateString('en-GB', {
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
    });
  };

  if (isLoading) {
    return null;
  }

  return (
    <div className="flex items-center gap-2 px-4 py-2 bg-white-ghost rounded-full">
      <IconClock className="h-4 w-4 text-blue-600" stroke={1.5} />
      <div className="text-sm text-gray-600 flex items-center">
        Last data update:
        {lastDate ? (
          <span className="text-blue-600 font-medium ml-2">{formatDate(lastDate)}</span>
        ) : (
          <span className="text-red-500 italic font-medium ml-2">no data</span>
        )}
      </div>
    </div>
  );
};

export default LastUpdatePassport;
