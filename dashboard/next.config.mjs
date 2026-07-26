/** @type {import('next').NextConfig} */
const nextConfig = {
  // Standalone output: the Docker build (dashboard/Dockerfile) copies just
  // .next/standalone + .next/static instead of the whole node_modules tree.
  output: "standalone",
};

export default nextConfig;
