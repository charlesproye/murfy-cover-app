'use client';

import React, { useEffect, useRef } from 'react';
import Image from 'next/image';
import { IconLayoutDashboard, IconWorld, IconWallet, IconCar } from '@tabler/icons-react';
import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { Toaster } from 'sonner';
import SearchBar from '@/components/common/search';
import Logout from '@/components/auth/Logout';
import { useAuth } from '@/contexts/AuthContext';
import ChangeFleet from '@/components/layout/ChangeFleet';
import LastUpdate from '@/components/layout/last-update/LastUpdate';
import LastUpdatePassport from '@/components/layout/last-update/LastUpdatePassport';
import Help from '@/components/layout/Help';

export default function DashboardLayout({
  children,
}: {
  children: React.ReactNode;
}): React.ReactElement {
  const real_pathname = usePathname();
  const { company, user, fleet } = useAuth();
  const vin = real_pathname.includes('/passport/')
    ? real_pathname.split('/').pop()
    : undefined;
  const scrollableContainerRef = useRef<HTMLDivElement>(null);

  const isActive = (pathname: string): boolean =>
    pathname === ''
      ? real_pathname === '/dashboard'
      : real_pathname === `/dashboard/${pathname}`;

  // Scroll to top of the scrollable container when navigating to /dashboard/global
  useEffect(() => {
    if (scrollableContainerRef.current) {
      scrollableContainerRef.current.scrollTop = 0;
    }
  }, [real_pathname]);

  return (
    <div className="flex flex-row min-h-[calc(100dvh)] h-[calc(100dvh)] overflow-hidden">
      <div className="flex bg-white dark:bg-dark-white h-[calc(100dvh)] flex-col w-14 sm:w-40 md:w-56 shrink-0">
        <div className="flex flex-col text-black dark:text-dark-dark px-2 overflow-auto z-20 h-full">
          <Link href="/dashboard">
            <div className="flex items-center mt-3 mb-3">
              {company?.name !== 'ituran' && (
                <Image
                  alt="logo-battery-green"
                  priority
                  width={34}
                  height={34}
                  src="/logo/logo-battery-green.webp"
                />
              )}
              <p className="font-bold text-xl hidden sm:contents">
                {company?.name === 'ituran' ? (
                  <img src="/logo/ituran.png" alt="logo-ituran" className="w-24" />
                ) : (
                  'EValue'
                )}
              </p>
            </div>
          </Link>
          <hr className="w-7 sm:w-28 md:w-40 mx-auto mt-2 border-cool-gray-200" />
          <div className="mt-6 ml-0 sm:ml-2">
            <p className="hidden sm:block uppercase text-gray dark:text-dark-gray-light text-xs tracking-widest">
              {' '}
              Dashboard{' '}
            </p>
            <Link href="/dashboard">
              <div
                className={`${
                  isActive('')
                    ? 'bg-white-clair dark:bg-dark-white-clair text-bleu-vif dark:text-dark-bleu-vif rounded-lg ml-0 px-2'
                    : 'ml-2'
                } cursor-pointer flex h-10 mt-2 gap-2 items-center`}
              >
                <IconLayoutDashboard
                  className={`w-[22px] h-[22px] sm:w-[18px] sm:h-[18px] ${
                    isActive('')
                      ? 'text-bleu-vif dark:text-dark-bleu-vif'
                      : 'text-gray dark:text-dark-gray'
                  }`}
                />
                <p
                  className={`hidden sm:block text-sm ${
                    isActive('')
                      ? 'text-bleu-vif dark:text-dark-bleu-vif font-semibold'
                      : 'text-gray dark:text-dark-gray font-thin'
                  }`}
                >
                  {' '}
                  Home{' '}
                </p>
              </div>
            </Link>
            <Link href={`/dashboard/global`}>
              <div
                className={`${
                  isActive('global')
                    ? 'bg-white-clair dark:bg-dark-white-clair text-bleu-vif dark:text-dark-bleu-vif rounded-lg ml-0 px-2'
                    : 'ml-2'
                } cursor-pointer flex h-10 gap-2 items-center relative`}
              >
                <IconWorld
                  className={`w-[22px] h-[22px] sm:w-[18px] sm:h-[18px] ${
                    isActive('global')
                      ? 'text-bleu-vif dark:text-dark-bleu-vif'
                      : 'text-gray dark:text-dark-gray'
                  }`}
                />
                <p
                  className={`hidden sm:block text-sm my-auto ${
                    isActive('global')
                      ? 'text-bleu-vif dark:text-dark-bleu-vif font-semibold'
                      : 'text-gray dark:text-dark-gray font-thin'
                  }`}
                >
                  Global
                </p>
              </div>
            </Link>
            <Link href={`/dashboard/favorites`}>
              <div
                className={`${
                  isActive('favorites')
                    ? 'bg-white-clair dark:bg-dark-white-clair text-bleu-vif dark:text-dark-bleu-vif rounded-lg ml-0 px-2'
                    : 'ml-2'
                } cursor-pointer flex h-10 gap-2 items-center relative`}
              >
                <IconCar
                  className={`w-[22px] h-[22px] sm:w-[18px] sm:h-[18px] ${
                    isActive('favorites')
                      ? 'text-bleu-vif dark:text-dark-bleu-vif'
                      : 'text-gray dark:text-dark-gray'
                  }`}
                />
                <p
                  className={`hidden sm:block text-sm my-auto ${
                    isActive('favorites')
                      ? 'text-bleu-vif dark:text-dark-bleu-vif font-semibold'
                      : 'text-gray dark:text-dark-gray font-thin'
                  }`}
                >
                  Favorites
                </p>
              </div>
            </Link>
          </div>
          <hr className="w-7 sm:w-28 md:w-40 mx-auto mt-4 border-cool-gray-200" />
          <div className="mt-0 sm:mt-6 ml-0 sm:ml-2">
            <p className="hidden sm:block uppercase text-gray dark:text-dark-gray-light text-xs tracking-widest">
              {' '}
              Financial{' '}
            </p>
            {/* <Link href="/dashboard/finance/individual">
              <div
                className={`${
                  isActive('finance/individual')
                    ? 'bg-white-clair dark:bg-dark-white-clair text-bleu-vif dark:text-dark-bleu-vif rounded-lg ml-0 px-2'
                    : 'ml-2'
                } cursor-pointer flex h-10 mt-2 gap-2 items-center`}
              >
                <IconReportMoney
                  className={`w-[22px] h-[22px] sm:w-[18px] sm:h-[18px] ${
                    isActive('finance/individual')
                      ? 'text-bleu-vif dark:text-dark-bleu-vif'
                      : 'text-gray dark:text-dark-gray'
                  }`}
                />
                <p
                  className={`hidden sm:block text-sm ${
                    isActive('finance/individual')
                      ? 'text-bleu-vif dark:text-dark-bleu-vif font-semibold'
                      : 'text-gray dark:text-dark-gray font-thin'
                  }`}
                >
                  {' '}
                  Individual{' '}
                </p>
              </div>
            </Link> */}
            <Link href={`/dashboard/finance/global`}>
              <div
                className={`${
                  isActive('finance/global')
                    ? 'bg-white-clair dark:bg-dark-white-clair text-bleu-vif dark:text-dark-bleu-vif rounded-lg ml-0 px-2'
                    : 'ml-2'
                } cursor-pointer flex h-10 gap-2 items-center relative`}
              >
                <IconWallet
                  className={`w-[22px] h-[22px] sm:w-[18px] sm:h-[18px] ${
                    isActive('finance/global')
                      ? 'text-bleu-vif dark:text-dark-bleu-vif'
                      : 'text-gray dark:text-dark-gray'
                  }`}
                />
                <p
                  className={`hidden sm:block text-sm my-auto ${
                    isActive('finance/global')
                      ? 'text-bleu-vif dark:text-dark-bleu-vif font-semibold'
                      : 'text-gray dark:text-dark-gray font-thin'
                  }`}
                >
                  Global
                </p>
              </div>
            </Link>
          </div>
          <hr className="w-7 sm:w-28 md:w-40 mx-auto mt-4 border-cool-gray-200 mb-3" />
          {!isActive('global') && <ChangeFleet />}
          <div className="flex flex-col mt-auto mb-4 ml-2 gap-2">
            <Help />
            <Logout />
          </div>
        </div>
      </div>
      <div
        ref={scrollableContainerRef}
        className="flex flex-col grow min-h-[calc(100dvh)] h-full overflow-auto overflow-x-hidden border-l border-gray-background scroll-smooth"
      >
        <div className="bg-white flex h-14 sm:h-[70px] px-8">
          <div className="flex w-full items-center min-h-[70px]">
            <div className="flex-1">
              <SearchBar />
            </div>
            <div className="hidden lg:flex flex-1 justify-center">
              {real_pathname.includes('/passport/') ? (
                <LastUpdatePassport vin={vin} />
              ) : (
                <LastUpdate fleetId={fleet?.id} />
              )}
            </div>
            <div className="flex-1 flex justify-end gap-2 items-center">
              <div className="flex ml-3 gap-2">
                <Image
                  alt="logo-battery-green"
                  className="object-cover w-auto h-auto"
                  width={30}
                  height={30}
                  src="/logo/logo-battery-green.webp"
                />
                <div className="flex flex-col justify-center">
                  <p className="text-sm font-semibold">{user?.first_name}</p>
                  <p className="text-xs text-gray">{user?.last_name}</p>
                </div>
              </div>
            </div>
          </div>
        </div>
        <div className="grow lg:px-8 py-4 px-4 max-w-(--breakpoint-xl) w-full mx-auto">
          <Toaster position="bottom-right" richColors />
          {children}
        </div>
      </div>
    </div>
  );
}
