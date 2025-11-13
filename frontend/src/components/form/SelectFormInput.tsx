'use client';

import React from 'react';
import { useFormContext, Controller } from 'react-hook-form';
import { Label } from '@/components/ui/label';
import { cn } from '@/lib/utils';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';

interface SelectFormInputProps {
  name: string;
  label?: string;
  placeholder?: string;
  required?: boolean;
  disabled?: boolean;
  className?: string;
  labelClassName?: string;
  selectClassName?: string;
  error?: string;
  showError?: boolean;
  onChange2?: (value: string) => void;
  options: Array<{ value: string; label: string }>;
}

export const SelectFormInput: React.FC<SelectFormInputProps> = ({
  name,
  label,
  placeholder,
  required = false,
  disabled = false,
  className,
  labelClassName,
  selectClassName,
  error,
  showError = true,
  onChange2,
  options,
}) => {
  const {
    control,
    formState: { errors },
  } = useFormContext();
  const fieldError = errors[name]?.message as string;
  const displayError = error || fieldError;

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
          <Select
            disabled={disabled}
            onValueChange={(value) => {
              field.onChange(value);
              onChange2?.(value);
            }}
            value={field.value || ''}
          >
            <SelectTrigger
              className={cn(
                'w-full',
                displayError && showError && 'border-red-500 focus:ring-red-500',
                selectClassName,
              )}
            >
              <SelectValue placeholder={placeholder} />
            </SelectTrigger>
            <SelectContent>
              {options.map((option) => (
                <SelectItem key={option.value} value={option.value}>
                  {option.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        )}
      />

      {displayError && showError && (
        <p className="text-sm text-red-500">{displayError}</p>
      )}
    </div>
  );
};
