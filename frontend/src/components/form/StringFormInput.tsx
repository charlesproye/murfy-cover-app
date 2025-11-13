'use client';

import React from 'react';
import { useFormContext, Controller } from 'react-hook-form';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { cn } from '@/lib/utils';

interface StringInputProps {
  name: string;
  label?: string;
  placeholder?: string;
  required?: boolean;
  disabled?: boolean;
  className?: string;
  labelClassName?: string;
  inputClassName?: string;
  error?: string;
  showError?: boolean;
  onValueChange?: (value: string) => void;
  type?: string;
}

export const StringFormInput: React.FC<StringInputProps> = ({
  name,
  label,
  placeholder,
  required = false,
  disabled = false,
  className,
  labelClassName,
  inputClassName,
  error,
  showError = true,
  onValueChange,
  type = 'text',
}) => {
  const {
    control,
    formState: { errors },
  } = useFormContext();
  const fieldError = errors[name]?.message as string;
  const displayError = error || fieldError;

  // const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
  //   // Prevent Enter key from bubbling up to parent elements
  //   if (e.key === 'Enter') {
  //     e.preventDefault();
  //     e.stopPropagation();
  //   }
  // };

  return (
    <div className={cn('space-y-1', className)}>
      {label && (
        <Label
          htmlFor={name}
          className={cn(
            'text-sm font-medium leading-none peer-disabled:cursor-not-allowed peer-disabled:opacity-70',
            labelClassName,
          )}
        >
          {label}
          {required && <span className="text-red-500 ml-1">*</span>}
        </Label>
      )}

      <Controller
        name={name}
        control={control}
        render={({ field }) => (
          <Input
            {...field}
            id={name}
            type={type}
            placeholder={placeholder}
            disabled={disabled}
            className={cn(
              'w-full',
              inputClassName,
              displayError && showError && 'border-red-500 focus-visible:ring-red-500',
            )}
            onChange={(e) => {
              field.onChange(e);
              onValueChange?.(e.target.value);
            }}
            // onKeyDown={handleKeyDown}
            value={field.value ?? ''}
          />
        )}
      />

      {displayError && showError && (
        <p className="text-sm text-red-500">{displayError}</p>
      )}
    </div>
  );
};
