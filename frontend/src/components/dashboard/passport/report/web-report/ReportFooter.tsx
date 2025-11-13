import Image from 'next/image';
import React from 'react';

const ReportFooter: React.FC = () => {
  return (
    <div className="bg-white rounded-bl-lg rounded-br-lg border-t border-gray/20 p-4 shadow-sm flex flex-col max-w-4xl w-full">
      <div className="flex justify-between ml-6 w-full">
        <Image
          src="/logo/bib-pine-green.svg"
          alt="Bib Pine Green Logo"
          width={96}
          height={96}
        />
        <div className="flex items-end text-gray">
          <p className="text-xs">Issued Date: {new Date().toLocaleDateString()}</p>
        </div>
      </div>
    </div>
  );
};

export default ReportFooter;
