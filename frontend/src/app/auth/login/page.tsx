'use client';

import LoginForm from '@/components/auth/LoginForm';
import Image from 'next/image';
import React from 'react';

const Login = (): React.ReactElement => {
  return (
    <div className="flex flex-col md:flex-row w-screen h-screen">
      <div className="w-full md:w-1/2 h-full flex items-center justify-center">
        <div className="relative  flex justify-center">
          <div className="p-8 md:p-24">
            <div className="flex flex-col items-center">
              <Image
                src="/logo/logo-battery-green.webp"
                priority
                className="-mt-[9px] -ml-4"
                width={100}
                height={100}
                alt="logo battery green"
              />
              <h1 className="text-3xl font-semibold -ml-1"> Welcome back to EValue</h1>
            </div>
            <p className="sm:mt-1 text-center">Nice to see you again</p>
            <LoginForm />
          </div>
        </div>
        <div className="flex justify-between absolute left-0 bottom-0 w-full md:w-1/2 mb-4 px-4">
          <div className="flex items-center">
            <a
              className="flex items-center"
              href="https://www.linkedin.com/company/bib-batteries/posts/?feedView=all"
            >
              <Image
                src="/logo/logo-battery-green.webp"
                width={30}
                height={30}
                alt="logo battery green"
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
      <div className="hidden md:block p-3 w-full md:w-1/2 relative">
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

export default Login;
