'use client';

import React from 'react';
import { useFormContext, Controller } from 'react-hook-form';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { cn } from '@/lib/utils';

interface NumberFormInputProps {
  name: string;
  label?: string;
  placeholder?: string;
  min?: number;
  max?: number;
  step?: number;
  required?: boolean;
  disabled?: boolean;
  className?: string;
  labelClassName?: string;
  inputClassName?: string;
  error?: string;
  showError?: boolean;
  onValueChange?: (value: number | undefined) => void;
}

export const NumberFormInput: React.FC<NumberFormInputProps> = ({
  name,
  label,
  placeholder,
  min,
  max,
  step = 1,
  required = false,
  disabled = false,
  className,
  labelClassName,
  inputClassName,
  error,
  showError = true,
  onValueChange,
}) => {
  const {
    control,
    formState: { errors },
  } = useFormContext();
  const fieldError = errors[name]?.message as string;
  const displayError = error || fieldError;

  const handleInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value;

    // Allow empty string, valid numbers, and minus sign for negative numbers
    if (value === '' || value === '-') {
      onValueChange?.(undefined);
      return;
    }

    const numValue = parseFloat(value);

    // Check if it's a valid number
    if (!isNaN(numValue)) {
      // Apply min/max constraints
      let constrainedValue = numValue;
      if (min !== undefined && numValue < min) {
        constrainedValue = min;
      }
      if (max !== undefined && numValue > max) {
        constrainedValue = max;
      }

      onValueChange?.(constrainedValue);
    }
  };

  const handleBlur = (e: React.FocusEvent<HTMLInputElement>) => {
    const value = e.target.value;

    // On blur, if the value is just a minus sign, clear it
    if (value === '-') {
      e.target.value = '';
      onValueChange?.(undefined);
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    // Prevent Enter key from bubbling up to parent elements
    if (e.key === 'Enter') {
      e.preventDefault();
      e.stopPropagation();
    }
  };

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
            type="number"
            placeholder={placeholder}
            min={min}
            max={max}
            step={step}
            disabled={disabled}
            className={cn(
              'w-full',
              displayError && showError && 'border-red-500 focus-visible:ring-red-500',
              inputClassName,
            )}
            onChange={(e) => {
              field.onChange(e);
              handleInputChange(e);
            }}
            onBlur={(e) => {
              field.onBlur();
              handleBlur(e);
            }}
            onKeyDown={handleKeyDown}
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
