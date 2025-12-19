'use client';

import { cn } from '@/lib/staticData';
import Image from 'next/image';
import React from 'react';

const LandingFooter = ({ className }: { className?: string }): React.ReactElement => {
  return (
    <div className={cn('flex justify-between w-full', className)}>
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
  );
};

export default LandingFooter;
