import { DownloadPropsExtremum } from '@/interfaces/common/download';
import { IconDownload } from '@tabler/icons-react';
import React from 'react';
import { ROUTES } from '@/routes';
import fetchWithAuth from '@/services/fetchWithAuth';
import { TableExtremumResult } from '@/interfaces/dashboard/home/table/TablebrandResult';

const DownloadExtremum = ({
  fleet,
  quality,
  filename = 'export',
  className = '',
}: DownloadPropsExtremum): React.ReactElement => {
  const downloadCSV = async (): Promise<void> => {
    const url = `${ROUTES.TABLE_EXTREMUM}?fleet_id=${fleet}&brand=All&extremum=${quality}`;
    const response = (await fetchWithAuth(url)) as TableExtremumResult | null;

    if (!response || response.vehicles?.length === 0) return;

    // Get headers from first object keys
    const headers = Object.keys(response.vehicles[0]);
    // Create CSV content
    const csvContent = [
      // Headers row
      headers.join(','),
      // Data rows
      ...response.vehicles.map((row) =>
        headers
          .map((header) => {
            const value = row[header as keyof TableExtremumResult['vehicles'][0]];
            // Handle special cases (null, undefined, numbers, strings with commas)
            if (value === null || value === undefined) return 'Unknown';
            if (typeof value === 'number') return value.toString();
            return `"${String(value)}"`; // Wrap strings in quotes to handle commas
          })
          .join(','),
      ),
    ].join('\n');

    // Create and trigger download
    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');
    const urlBlob = URL.createObjectURL(blob);

    link.setAttribute('href', urlBlob);
    link.setAttribute('download', `${filename}.csv`);
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  return <IconDownload className={`cursor-pointer ${className}`} onClick={downloadCSV} />;
};

export default DownloadExtremum;
