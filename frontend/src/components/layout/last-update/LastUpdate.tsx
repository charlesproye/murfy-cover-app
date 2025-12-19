import React from 'react';
import { IconClock } from '@tabler/icons-react';
import { ROUTES } from '@/routes';
import useSWR from 'swr';
import fetchWithAuth from '@/services/fetchWithAuth';

interface LastUpdateProps {
  fleetId: string | undefined;
}

const LastUpdate: React.FC<LastUpdateProps> = ({ fleetId }) => {
  const { data: last_timestamp_with_data, isLoading: isLoadingLastTimestampWithData } =
    useSWR(
      `${ROUTES.LAST_TIMESTAMP_WITH_DATA}?fleet_id=${fleetId ?? ''}`,
      fetchWithAuth<string>,
    );

  const formatDate = (date: string): string => {
    const d = new Date(date);
    return d.toLocaleDateString('en-GB', {
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
    });
  };

  if (isLoadingLastTimestampWithData) {
    return null;
  }

  return (
    <div className="flex items-center gap-2 px-4 py-2 bg-white-ghost rounded-full">
      <IconClock className="h-4 w-4 text-blue-600" stroke={1.5} />
      <div className="text-sm text-gray-600 flex items-center">
        Last data update:
        {last_timestamp_with_data ? (
          <span className="text-blue-600 font-medium ml-2">
            {formatDate(last_timestamp_with_data)}
          </span>
        ) : (
          <span className="text-red-500 italic font-medium ml-2">no data</span>
        )}
      </div>
    </div>
  );
};

export default LastUpdate;
