import React from 'react';
import { View, Text, Link } from '@react-pdf/renderer';
import { LanguageEnum } from '@/components/entities/flash-report/forms/schema';
import { pdfTexts } from '@/components/pdf/pdf_texts';

const sectionStyle = {
  backgroundColor: '#fff',
  borderRadius: 12,
  paddingLeft: 12,
  paddingRight: 12,
  paddingTop: 6,
  paddingBottom: 6,
  marginBottom: 4,
  border: '1px solid #E5E7EB',
};

const titleStyle = {
  fontSize: 12,
  fontWeight: 'medium',
  marginBottom: 4,
  color: '#111827',
};

const textStyle = {
  fontSize: 8,
  color: '#374151',
  marginBottom: 2,
};

const PdfDetailedInformationFlash: React.FC<{
  language?: LanguageEnum;
  warranty_date?: number;
  warranty_km?: number;
}> = ({ language, warranty_date, warranty_km }) => {
  const texts = pdfTexts[language || LanguageEnum.EN];
  return (
    <View>
      {/* State of Health (SoH) Flash Report */}
      <View style={{ ...sectionStyle }}>
        <Text style={titleStyle}>{texts.soh_information_flash.title}</Text>
        <Text style={textStyle}>
          {texts.soh_information_flash.explanation} (
          <Link
            src="https://unece.org/sites/default/files/2023-01/ECE_TRANS_180a22f.pdf"
            style={{ color: '#2563eb', textDecoration: 'underline' }}
          >
            https://unece.org/sites/default/files/2023-01/ECE_TRANS_180a22f.pdf
          </Link>
          ).
        </Text>
        <Text style={textStyle}>
          {texts.soh_information_flash.explanation_2} (
          <Link
            src="mailto:contact@bib-batteries.fr"
            style={{ color: '#2563eb', textDecoration: 'underline' }}
          >
            contact@bib-batteries.fr
          </Link>
          ).
        </Text>
      </View>
      {/* Vehicle & Battery Warranty */}
      <View style={sectionStyle}>
        <Text style={titleStyle}>{texts.vehicle_and_battery_warranty.title}</Text>
        <Text style={textStyle}>{texts.vehicle_and_battery_warranty.explanation}</Text>
        <Text style={textStyle}>{texts.vehicle_and_battery_warranty.explanation_2}</Text>
        <Text style={textStyle}>
          {texts.vehicle_and_battery_warranty.explanation_3(warranty_date, warranty_km)}
        </Text>
      </View>
      {/* Range Insights */}
      <View style={sectionStyle}>
        <Text style={titleStyle}>{texts.range_insights.title}</Text>
        <Text style={textStyle}>{texts.range_insights.explanation}</Text>
      </View>
      {/* Report Validity */}
      <View style={sectionStyle}>
        <Text style={titleStyle}>{texts.report_validity.title}</Text>
        <Text style={textStyle}>
          {texts.report_validity.explanation}{' '}
          <Link
            src="https://www.bib-batteries.fr"
            style={{ color: '#2563eb', textDecoration: 'underline' }}
          >
            www.bib-batteries.fr
          </Link>
          .
        </Text>
      </View>
    </View>
  );
};

export default PdfDetailedInformationFlash;
