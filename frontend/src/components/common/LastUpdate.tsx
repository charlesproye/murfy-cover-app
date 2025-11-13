import React from 'react';
import { IconClock } from '@tabler/icons-react';

interface LastUpdateProps {
  lastDate?: string | null;
  activationStatus: boolean | undefined;
}

const LastUpdate: React.FC<LastUpdateProps> = ({ lastDate, activationStatus }) => {
  const formatDate = (date: string): string => {
    const d = new Date(date);
    return d.toLocaleDateString('en-GB', {
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
    });
  };

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
        {activationStatus && (
          <span className="animate-ping w-2 h-2 rounded-full bg-primary ml-3"></span>
        )}
      </div>
    </div>
  );
};

export default LastUpdate;
