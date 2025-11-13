import { ImagePdfProps } from '@/interfaces/pdf/DataPdf';
import { useState } from 'react';

const resizeImage = (
  img: HTMLImageElement,
  maxWidth: number,
  maxHeight: number,
): HTMLCanvasElement => {
  const canvas = document.createElement('canvas');
  let width = img.width;
  let height = img.height;

  // Calculer le facteur de mise à l'échelle
  const scaleFactor = Math.min(maxWidth / width, maxHeight / height);

  // Si l'image est plus grande que les dimensions maximales, on la redimensionne
  if (scaleFactor < 1) {
    width *= scaleFactor;
    height *= scaleFactor;
  } else {
    // Sinon, garder les dimensions originales
    width = img.width;
    height = img.height;
  }

  canvas.width = width;
  canvas.height = height;
  const ctx = canvas.getContext('2d', { willReadFrequently: true });
  if (ctx) {
    ctx.imageSmoothingEnabled = true;
    ctx.imageSmoothingQuality = 'high';
  }
  ctx?.drawImage(img, 0, 0, width, height);

  return canvas;
};

const GetImagePdf = ({ url }: ImagePdfProps): string | null | undefined => {
  const [imgSrc, setImgSrc] = useState<string | null>(null);

  const sampleData = {
    picture: {
      imageSrc: url,
      mimeType: 'image/jpg',
    },
  };

  if (sampleData.picture) {
    const tempImage = document.createElement('img');
    tempImage.crossOrigin = 'anonymous';

    tempImage.addEventListener('load', () => {
      if (sampleData.picture.mimeType && sampleData.picture.mimeType !== 'image/png') {
        const maxWidth = 595; // Largeur standard d'une page A4 en points
        const maxHeight = 842; // Hauteur standard d'une page A4 en points
        const resizedCanvas = resizeImage(tempImage, maxWidth, maxHeight);

        const pngDataUrl = resizedCanvas.toDataURL('image/png', 0.8);
        setImgSrc(pngDataUrl);
      }
    });

    tempImage.src = sampleData.picture.imageSrc;
    return imgSrc;
  }
  return null;
};

export default GetImagePdf;
