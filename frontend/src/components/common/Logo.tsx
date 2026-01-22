'use client';

import Image from 'next/image';
import React from 'react';

const Logo = ({ title = 'Bib batteries' }: { title?: string }): React.ReactElement => {
  return (
    <div className="flex flex-col items-center">
      <Image
        src="/logo/bib-pine-green.svg"
        priority
        width={100}
        height={100}
        alt="logo bib"
      />
      <h1 className="text-3xl font-semibold py-[5px]">{title}</h1>
    </div>
  );
};

export default Logo;
