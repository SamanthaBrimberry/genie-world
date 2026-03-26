// @ts-check
import {themes as prismThemes} from 'prism-react-renderer';

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'Genie World',
  tagline: 'Modular Python library for building, benchmarking, and optimizing Databricks Genie Spaces',
  favicon: 'img/favicon.ico',

  future: {
    v4: true,
  },

  url: 'https://samanthabrimberry.github.io',
  baseUrl: '/genie-world/',

  organizationName: 'SamanthaBrimberry',
  projectName: 'genie-world',
  trailingSlash: false,

  onBrokenLinks: 'throw',

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: './sidebars.js',
          editUrl:
            'https://github.com/SamanthaBrimberry/genie-world/tree/main/website/',
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      colorMode: {
        respectPrefersColorScheme: true,
      },
      navbar: {
        title: 'Genie World',
        items: [
          {
            type: 'docSidebar',
            sidebarId: 'docsSidebar',
            position: 'left',
            label: 'Getting Started',
          },
          {
            href: 'https://github.com/SamanthaBrimberry/genie-world',
            label: 'GitHub',
            position: 'right',
          },
        ],
      },
      footer: {
        style: 'dark',
        links: [
          {
            title: 'Docs',
            items: [
              {
                label: 'Getting Started',
                to: '/docs/getting-started/installation',
              },
              {
                label: 'User Guide',
                to: '/docs/user-guide/profiler',
              },
            ],
          },
          {
            title: 'More',
            items: [
              {
                label: 'GitHub',
                href: 'https://github.com/SamanthaBrimberry/genie-world',
              },
            ],
          },
        ],
        copyright: `Copyright © ${new Date().getFullYear()} Genie World. Built with Docusaurus.`,
      },
      prism: {
        theme: prismThemes.github,
        darkTheme: prismThemes.dracula,
        additionalLanguages: ['bash', 'python'],
      },
    }),
};

export default config;
