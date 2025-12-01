'use client';

import Image from 'next/image';
import React from 'react';

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
            <div className="flex flex-col items-center">
              <Image
                src="/logo/logo-battery-green.webp"
                priority
                className="-mt-[9px] -ml-4"
                width={100}
                height={100}
                alt="logo bib"
              />
              <h1 className="text-3xl font-semibold -ml-1">EValue Flash Report</h1>
            </div>
            {children}
          </div>
        </div>
        <div className="flex justify-between w-full mb-2 px-2 flex-shrink-0">
          <div className="flex items-center">
            <a
              className="flex items-center"
              href="https://www.linkedin.com/company/bib-batteries/posts/?feedView=all"
            >
              <Image
                src="/logo/logo-battery-green.webp"
                width={30}
                height={30}
                alt="logo bib"
              />
              <p className="text-sm text-primary ml-1"> Bib Batteries </p>
            </a>
          </div>
          <div className="flex items-center">
            <p className="text-sm">
              {' '}
              © <a href="https://bib-batteries.fr">Bib Batteries</a>{' '}
              {new Date().getFullYear()}{' '}
            </p>
          </div>
        </div>
      </div>
      <div className="hidden md:block p-3 w-full md:w-2/5 relative">
        <Image
          src="/auth/background-1.jpeg"
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
