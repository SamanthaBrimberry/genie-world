import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import Heading from '@theme/Heading';
import styles from './index.module.css';

const FeatureList = [
  {
    step: '01',
    title: 'Profile',
    description: 'Scan your Unity Catalog schema to discover column types, relationships, cardinality, and business-friendly synonyms.',
  },
  {
    step: '02',
    title: 'Build & Deploy',
    description: 'Generate a complete Genie Space config with data sources, SQL instructions, example Q&A pairs, and snippets.',
  },
  {
    step: '03',
    title: 'Benchmark & Tune',
    description: 'Run questions against your live space, evaluate accuracy, diagnose failures, and auto-tune to your target.',
  },
];

function Feature({step, title, description}) {
  return (
    <div className={clsx('col col--4')}>
      <div className={styles.featureCard}>
        <div className={styles.stepNumber}>{step}</div>
        <Heading as="h3">{title}</Heading>
        <p>{description}</p>
      </div>
    </div>
  );
}

function HomepageHeader() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <header className={styles.heroBanner}>
      <div className={styles.heroCard}>
        <Heading as="h1">{siteConfig.title}</Heading>
        <p>{siteConfig.tagline}</p>
        <div className={styles.buttons}>
          <Link className={styles.buttonPrimary} to="/docs/getting-started/installation">
            Get Started
          </Link>
          <Link className={styles.buttonSecondary} to="/docs/user-guide/profiler">
            User Guide
          </Link>
        </div>
      </div>
    </header>
  );
}

export default function Home() {
  return (
    <Layout
      title="Home"
      description="Modular Python library for building, benchmarking, and optimizing Databricks Genie Spaces">
      <HomepageHeader />
      <main>
        <section className={styles.features}>
          <div className="container">
            <div className="row">
              {FeatureList.map((props, idx) => (
                <Feature key={idx} {...props} />
              ))}
            </div>
          </div>
        </section>
      </main>
    </Layout>
  );
}
