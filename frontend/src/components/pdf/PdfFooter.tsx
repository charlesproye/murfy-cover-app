import React from 'react';
import { View, Text, Image } from '@react-pdf/renderer';
import { pdfTexts } from '@/components/pdf/pdf_texts';
import { LanguageEnum } from '@/components/entities/flash-report/forms/schema';

interface PdfFooterProps {
  language?: LanguageEnum;
}

const PdfFooter: React.FC<PdfFooterProps> = ({ language }) => {
  const texts = pdfTexts[language || LanguageEnum.EN];

  return (
    <View
      style={{
        flexDirection: 'row',
        justifyContent: 'space-between',
        borderRadius: 12,
        alignItems: 'center',
        marginTop: 'auto',
        paddingTop: 8,
        paddingBottom: 6,
        backgroundColor: '#fff',
        borderTop: '1px solid #E5E7EB',
      }}
    >
      <Image
        src="/logo/bib-pine-green.png"
        style={{
          height: 36,
          marginLeft: 16,
        }}
      />
      <View
        style={{
          display: 'flex',
          justifyContent: 'flex-end',
          alignItems: 'flex-end',
          marginRight: 16,
          height: '100%',
        }}
      >
        <Text
          style={{
            fontSize: 10,
            color: '#6B7280',
          }}
        >
          {texts.issued_date} {new Date().toLocaleDateString()}
        </Text>
      </View>
    </View>
  );
};

export default PdfFooter;
