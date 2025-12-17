'use client';

import useLogin from '@/hooks/auth/useLogin';
import React from 'react';
import VibratingButton from '@/components/form/VibratingButton';

const LoginForm = (): React.ReactElement => {
  const { email, setEmail, password, setPassword, error, handleSubmit, isLoading } =
    useLogin();

  return (
    <form className="mt-8 gap-4 flex flex-col" onSubmit={handleSubmit}>
      <div className="">
        <label
          className="block text-gray-700 text-sm font-medium mb-2 ml-4"
          htmlFor="email"
        >
          Email
        </label>
        <input
          id="email"
          type="email"
          autoComplete="email"
          placeholder="Enter your email"
          className="w-full outline-hidden bg-[#F2F2F2] rounded-xl p-3 placeholder:text-[#808080]"
          value={email}
          onChange={(e) => setEmail(e.target.value)}
        />
      </div>
      <div className="">
        <label
          className="block text-gray-700 text-sm font-medium mb-2 ml-4"
          htmlFor="password"
        >
          Password
        </label>
        <input
          id="password"
          type="password"
          autoComplete="current-password"
          placeholder="Enter your password"
          className="w-full outline-hidden bg-[#F2F2F2] rounded-xl p-3 placeholder:text-[#808080]"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
        />
      </div>
      {error && <p className="text-red-500 text-sm">{error}</p>}
      <div className="flex items-center justify-between">
        <VibratingButton
          isError={false}
          buttonText={'Login'}
          onClick={() => handleSubmit}
          loader={isLoading}
        />
      </div>
    </form>
  );
};

export default LoginForm;
