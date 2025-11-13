import { DownloadProps } from '@/interfaces/common/download';
import { IconDownload } from '@tabler/icons-react';
import React from 'react';

const Download = <T extends Record<string, unknown>>({
  data,
  filename = 'export',
  className = '',
}: DownloadProps<T>): React.ReactElement => {
  const downloadCSV = (): void => {
    if (!data || data.length === 0) return;

    // Get headers from first object keys
    const headers = Object.keys(data[0]);
    // Create CSV content
    const csvContent = [
      // Headers row
      headers.join(','),
      // Data rows
      ...data.map((row) =>
        headers
          .map((header) => {
            const value = row[header];
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
    const url = URL.createObjectURL(blob);

    link.setAttribute('href', url);
    link.setAttribute('download', `${filename}.csv`);
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  return <IconDownload className={`cursor-pointer ${className}`} onClick={downloadCSV} />;
};

export default Download;
