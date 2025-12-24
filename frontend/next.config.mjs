/** @type {import('next').NextConfig} */
const nextConfig = {
  output: 'standalone',
  poweredByHeader: false,
  compress: true,
  trailingSlash: true,
  skipMiddlewareUrlNormalize: true,
  skipTrailingSlashRedirect: true,
  images: {
    remotePatterns: [
      {
        protocol: 'https',
        hostname: 's3.fr-par.scw.cloud',
        pathname: '/**',
      },
      {
        protocol: 'https',
        hostname: 'bib-batteries-assets.s3.fr-par.scw.cloud',
        pathname: '/**',
      },
      {
        protocol: 'https',
        hostname: 'static-assets.tesla.com',
        pathname: '/**',
      },
      {
        protocol: 'https',
        hostname: 'www.tesla.com',
        pathname: '/**',
      },
      {
        protocol: 'https',
        hostname: 'images.caradisiac.com',
        pathname: '/**',
      },
      {
        protocol: 'https',
        hostname: 'www.opel.be',
        pathname: '/**',
      },
      {
        protocol: 'https',
        hostname: 'www.dsautomobiles.fr',
        pathname: '/**',
      },
      {
        protocol: 'https',
        hostname: 'www.pca-services.fr',
        pathname: '/**',
      },
      {
        protocol: 'https',
        hostname: 'cdn.automobile-propre.com',
        pathname: '/**',
      },
      {
        protocol: 'https',
        hostname: 'www.mercedes-benz.fr',
        pathname: '/**',
      },
      {
        protocol: 'https',
        hostname: 'www.peugeot.fr',
        pathname: '/**',
      },
    ],
  },
};

export default nextConfig;
