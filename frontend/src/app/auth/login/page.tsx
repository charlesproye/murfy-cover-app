'use client';

import LoginForm from '@/components/auth/LoginForm';
import Image from 'next/image';
import React from 'react';
import Logo from '@/components/common/Logo';
import LandingFooter from '@/components/common/LandingFooter';

const Login = (): React.ReactElement => {
  return (
    <div className="flex flex-col md:flex-row w-screen h-screen">
      <div className="w-full md:w-1/2 h-full flex items-center justify-center">
        <div className="relative  flex justify-center">
          <div className="p-8 md:p-24">
            <Logo title="Welcome back to EValue" />
            <p className="sm:mt-1 text-center">Nice to see you again</p>
            <LoginForm />
          </div>
        </div>
        <LandingFooter className="absolute left-0 bottom-0 md:w-1/2 mb-4 px-4" />
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
