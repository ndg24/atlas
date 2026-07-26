import Link from "next/link";

const LINKS = [
  { href: "/datasets", label: "Datasets" },
  { href: "/query", label: "Query" },
  { href: "/research", label: "Research" },
  { href: "/history", label: "History" },
];

export function Nav() {
  return (
    <header className="border-b border-neutral-200 dark:border-neutral-800">
      <div className="mx-auto flex max-w-6xl items-center gap-6 px-6 py-4">
        <Link href="/datasets" className="font-semibold tracking-tight">
          Atlas
        </Link>
        <nav className="flex gap-4 text-sm">
          {LINKS.map((link) => (
            <Link
              key={link.href}
              href={link.href}
              className="text-neutral-600 hover:text-neutral-900 dark:text-neutral-400 dark:hover:text-neutral-100"
            >
              {link.label}
            </Link>
          ))}
        </nav>
      </div>
    </header>
  );
}
