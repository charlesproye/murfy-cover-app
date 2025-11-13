# eValue Front App

Displaying eValue data on a beautiful web app, based on NextJS (app version).
Main technos used :

- styling: tailwind
- typescript

---

## Project overview

### Authentification / Authorization

We use a classic email+password authentification and httpOnly cookies for authorization.

#### Main features

##### HttpOnly cookies for tokens

- Tokens stored in httpOnly cookies (not accessible via JavaScript)
- Prevents XSS attacks from stealing tokens
- Automatic inclusion in requests

##### Encrypted storage for sensitive data

- Sensitive data encrypted using Web Crypto API
- PBKDF2 key derivation with 100,000 iterations
- AES-GCM encryption with random IVs

##### Automatic token refresh with secure handling

- SHA-256 hashes for data integrity
- Automatic corruption detection
- Secure data cleanup on errors

##### Session management with integrity verification

- Non-sensitive session data in signed cookies
- Automatic session validation
- Secure logout with cookie clearing

#### Environment Variables

Add to your `.env`:

```env
# FRONTEND: Encryption key for client-side data encryption
NEXT_PUBLIC_ENCRYPTION_KEY=your-32-character-encryption-key-here

# BACKEND: cookie settings
COOKIE_SECURE=true
COOKIE_DOMAIN=yourdomain.com
```

---

## Setup and run

### Install dependencies

We use pnpm in this project:

```
pnpm install
```

No need to have a .env file, unless you're trying to access "api-externe" backend not locally,
then add process.env.NEXT_PUBLIC_API_URL with the proper value.

### Run in dev mode

Get .env from admin, then:

```
pnpm dev
```

Open [http://localhost:3000](http://localhost:3000) with your browser to see the result.

---

## Good practices

- prettier is set up, please use format when saving to ensure code consistency

## Docker

Build:

```bash
docker compose build evalue-front
```

Build and push:

```
docker compose build evalue-front --push
```
