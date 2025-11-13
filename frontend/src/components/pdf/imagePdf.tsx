'use client';

import React from 'react';
import { View, Image } from '@react-pdf/renderer';
import { ImagePdfProps } from '@/interfaces/pdf/DataPdf';
import GetImagePdf from '@/utils/pdfImage';

function ImagePdf({ url }: ImagePdfProps): React.ReactElement {
  const imgSrc = GetImagePdf({ url });

  return (
    <View
      style={{
        height: 160,
        width: 240,
        display: 'flex',
        justifyContent: 'center',
        alignItems: 'center',
      }}
    >
      {imgSrc && (
        <Image
          src={imgSrc}
          style={{ width: '100%', height: '100%', objectFit: 'contain' }}
        />
      )}
    </View>
  );
}

export default ImagePdf;
