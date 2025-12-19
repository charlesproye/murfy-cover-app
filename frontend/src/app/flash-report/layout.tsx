'use client';

import Image from 'next/image';
import React from 'react';
import Logo from '@/components/common/Logo';
import LandingFooter from '@/components/common/LandingFooter';

const FlashReportLayout = ({
  children,
}: {
  children: React.ReactNode;
}): React.ReactElement => {
  return (
    <div className="flex flex-col md:flex-row w-screen h-screen">
      <div className="w-full md:w-3/5 h-full flex flex-col relative">
        <div className="flex-1 overflow-y-auto flex items-center justify-center">
          <div className="flex flex-col justify-center p-8 w-full gap-4">
            <Logo title="EValue Flash Report" />
            {children}
          </div>
        </div>
        <LandingFooter className="mb-2 px-2 flex-shrink-0" />
      </div>
      <div className="hidden md:block p-3 w-full md:w-2/5 relative">
        <Image
          src="/auth/background-landing.jpeg"
          alt="bib presentation"
          width={400}
          height={400}
          className="object-cover w-full h-full rounded-md"
        />
      </div>
    </div>
  );
};

export default FlashReportLayout;
