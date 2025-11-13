import { useState, useEffect } from 'react';

export const useBrowserLanguage = () => {
  const [browserLanguage, setBrowserLanguage] = useState<string>('en');

  useEffect(() => {
    if (typeof window !== 'undefined') {
      const browserLang = navigator.language.split('-')[0];
      setBrowserLanguage(browserLang);
    }
  }, []);

  return browserLanguage;
};
