import React from 'react';

const FinancialIndividualPage = (): React.ReactElement => {
  return (
    <div className="h-[calc(100vh-120px)] flex flex-col items-center justify-center">
      <h1 className="text-5xl text-primary font-bold mb-8 animate-pulse">Coming Soon</h1>
      <p className="text-gray text-lg mb-8">
        We're working hard to bring you something amazing. Stay tuned!
      </p>
      <div className="w-64 h-2 bg-gray-200 rounded-full overflow-hidden">
        <div className="h-full bg-primary rounded-full animate-loading"></div>
      </div>
    </div>
  );
};

export default FinancialIndividualPage;
