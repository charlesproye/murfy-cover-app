'use client';

import Image from 'next/image';
import React from 'react';

const Logo = ({ title = 'EValue' }: { title?: string }): React.ReactElement => {
  return (
    <div className="flex flex-col items-center">
      <Image
        src="/logo/logo-battery-green.webp"
        priority
        className="-mt-[9px] -ml-4"
        width={100}
        height={100}
        alt="logo bib"
      />
      <h1 className="text-3xl font-semibold -ml-1">{title}</h1>
    </div>
  );
};

export default Logo;
