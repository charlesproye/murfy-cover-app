'use client';

import React, { useState, useRef, useEffect } from 'react';
import { ROUTES } from '@/routes';
import { fetchWithoutAuth } from '@/services/fetchWithoutAuth';
import { Input } from '@/components/ui/input';
import {
  IconDownload,
  IconShieldCheck,
  IconX,
  IconInfoCircle,
} from '@tabler/icons-react';
import { cn } from '@/lib/staticData';

interface VerificationResponse {
  verified: boolean;
  pdf_url: string | null;
  report_date: string | null;
}

interface ReportVerificationProps {
  reportUuid: string;
}

const VIN_DIGITS = 4;

export const ReportVerification = ({ reportUuid }: ReportVerificationProps) => {
  const [otp, setOtp] = useState<string[]>(Array(VIN_DIGITS).fill(''));
  const [isLoading, setIsLoading] = useState(false);
  const [result, setResult] = useState<VerificationResponse | null>(null);
  const [verificationFailed, setVerificationFailed] = useState(false);
  const inputRefs = useRef<(HTMLInputElement | null)[]>([]);

  // Focus last input after failed verification (once loading is done)
  useEffect(() => {
    if (verificationFailed && !isLoading) {
      inputRefs.current[VIN_DIGITS - 1]?.focus();
    }
  }, [verificationFailed, isLoading]);

  const submitVerification = async (code: string) => {
    if (code.length !== VIN_DIGITS || isLoading) return;

    setIsLoading(true);
    setVerificationFailed(false);

    try {
      const response = await fetchWithoutAuth<VerificationResponse>(
        `${ROUTES.VERIFY_REPORT}/${reportUuid}`,
        { method: 'POST', body: { vin_last_4: code } },
      );
      if (response.verified) {
        setResult(response);
      } else {
        setVerificationFailed(true);
      }
    } catch {
      setVerificationFailed(true);
    } finally {
      setIsLoading(false);
    }
  };

  const handleInputChange = (index: number, value: string) => {
    if (value && !/^[0-9]$/.test(value)) return;
    const newOtp = [...otp];
    newOtp[index] = value;
    setOtp(newOtp);
    setVerificationFailed(false);
    if (value && index < VIN_DIGITS - 1) {
      inputRefs.current[index + 1]?.focus();
    } else if (value && index === VIN_DIGITS - 1) {
      const fullCode = newOtp.join('');
      if (fullCode.length === VIN_DIGITS) {
        submitVerification(fullCode);
      }
    }
  };

  const handleKeyDown = (index: number, e: React.KeyboardEvent) => {
    if (e.key === 'Backspace' && !otp[index] && index > 0) {
      inputRefs.current[index - 1]?.focus();
    }
  };

  const handlePaste = (e: React.ClipboardEvent) => {
    e.preventDefault();
    const pasted = e.clipboardData
      .getData('text')
      .replace(/[^0-9]/g, '')
      .slice(0, VIN_DIGITS);
    if (pasted) {
      const newOtp = pasted
        .split('')
        .concat(Array(VIN_DIGITS).fill(''))
        .slice(0, VIN_DIGITS);
      setOtp(newOtp);
      setVerificationFailed(false);
      if (pasted.length === VIN_DIGITS) {
        submitVerification(pasted);
      } else {
        inputRefs.current[Math.min(pasted.length, VIN_DIGITS - 1)]?.focus();
      }
    }
  };

  return (
    <div className="bg-white rounded-xl shadow-lg border border-gray-100">
      <div className="bg-blue-50/50 p-4 border-b border-blue-100 flex gap-3">
        <IconInfoCircle className="w-5 h-5 text-blue-500 shrink-0 mt-0.5" />
        <p className="text-sm text-blue-700">
          Enter the last {VIN_DIGITS} digits of the VIN to verify this report.
        </p>
      </div>

      <div className="p-6">
        {result?.verified ? (
          <div className="text-center space-y-4">
            <div className="w-16 h-16 bg-green-50 rounded-full flex items-center justify-center mx-auto">
              <IconShieldCheck className="w-8 h-8 text-green-600" />
            </div>
            <div>
              <h2 className="text-lg font-bold">Report Verified</h2>
              <p className="text-gray-500 text-sm">Generated on {result.report_date}</p>
            </div>
            {result.pdf_url && (
              <a
                href={result.pdf_url}
                target="_blank"
                rel="noopener noreferrer"
                className="flex items-center justify-center gap-2 bg-primary text-primary-foreground w-full h-10 rounded-md hover:bg-primary/90"
              >
                <IconDownload className="w-4 h-4" />
                Download PDF
              </a>
            )}
          </div>
        ) : (
          <div className="flex flex-col items-center gap-3">
            <div className="relative flex gap-3 items-center">
              {otp.map((digit, i) => (
                <Input
                  key={i}
                  ref={(el) => {
                    inputRefs.current[i] = el;
                  }}
                  type="text"
                  inputMode="numeric"
                  pattern="[0-9]*"
                  value={digit}
                  onChange={(e) => handleInputChange(i, e.target.value)}
                  onKeyDown={(e) => handleKeyDown(i, e)}
                  onPaste={handlePaste}
                  disabled={isLoading}
                  className={cn(
                    'w-14 h-16 text-center text-3xl font-mono',
                    verificationFailed && 'border-red-400 bg-red-50',
                  )}
                  maxLength={1}
                  autoComplete="off"
                />
              ))}
              {verificationFailed && (
                <IconX className="absolute -right-9 w-6 h-6 text-red-500" />
              )}
            </div>
            <div className="text-sm text-gray-500">
              WP0ZZZ99ZTS39<b>2124</b>: <b>2124</b>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};
