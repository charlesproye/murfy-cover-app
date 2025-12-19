'use client';

import LandingFooter from '@/components/common/LandingFooter';
import Logo from '@/components/common/Logo';
import Image from 'next/image';
import React from 'react';

const TeslaActivationLayout = ({
  children,
}: {
  children: React.ReactNode;
}): React.ReactElement => {
  return (
    <div className="flex flex-col md:flex-row w-screen h-screen">
      <div className="w-full md:w-3/5 h-full flex items-center justify-center">
        <div className="flex flex-col justify-center p-8 w-full gap-4">
          <Logo title="Tesla Activation" />
          {children}
        </div>
        <LandingFooter className="absolute left-0 bottom-0 md:w-1/2 mb-4 px-4" />
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

export default TeslaActivationLayout;
