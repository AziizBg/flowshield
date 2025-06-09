/** @type {import('next').NextConfig} */
const nextConfig = {
  webpack: (config) => {
    config.resolve.fallback = { 
      fs: false, 
      path: false,
      crypto: false,
      stream: false,
      util: false,
      buffer: false,
      process: false
    };
    return config;
  },
}

module.exports = nextConfig 