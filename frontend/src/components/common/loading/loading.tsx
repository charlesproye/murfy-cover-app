import React from 'react';

export const Loading = (): React.ReactElement => {
  return (
    <div className="flex items-center justify-center min-h-screen bg-[#F7F6F9]">
      <div className="animate-spin rounded-full h-12 w-12 border-t-2 border-b-2 border-primary"></div>
    </div>
  );
};

export const LoadingTable = (): React.ReactElement => (
  <div className="flex items-center justify-center bg-white mt-4">
    <div className="animate-spin rounded-full h-12 w-12 border-t-2 border-b-2 border-primary"></div>
  </div>
);
