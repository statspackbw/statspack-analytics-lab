# Single sign-on: removing the duplicate login

The problem: SmartRegister embeds Analytics Lab in an iframe, and the user is asked to
sign in a second time.

The fix: SmartRegister's **backend** issues a short-lived, signed token. The iframe points
at `/sso?token=…`, Analytics Lab verifies the signature, signs the user in and renders the
dashboard. **No password is ever sent, and nothing sensitive is stored in the browser.**

Both systems stay exactly where they are. No hosting changes.

---

## 1. Share a secret

StatsPack sets `SSO_SECRET` on the Analytics Lab server. SmartRegister stores the same
value in its backend config (never in the Flutter app — see the warning in section 4).

Optional: `SSO_MAX_AGE` (default `120`) is how many seconds a token stays valid.

## 2. Token format

Two dot-separated parts:

```
base64url(JSON payload) + "." + HMAC_SHA256(base64url_payload, SSO_SECRET) as lowercase hex
```

Payload fields:

| Field | Required | Meaning |
|---|---|---|
| `api_key` | yes | The client's Analytics Lab API key — identifies which client's data to show |
| `email` | yes | The signed-in SmartRegister user. Their Lab account is created automatically on first use |
| `name` | no | Display name |
| `exp` | yes | Unix seconds when the token expires. Keep it short (60–120 s) |
| `nonce` | yes | Unique random string per token. A token can only be used **once** |

## 3. Generating the token (Node / Cloud Function)

```js
const crypto = require("crypto");

function labSsoUrl(email, name) {
  const payload = {
    api_key: process.env.ANALYTICS_LAB_API_KEY,
    email, name,
    exp: Math.floor(Date.now() / 1000) + 120,
    nonce: crypto.randomBytes(8).toString("hex"),
  };
  const body = Buffer.from(JSON.stringify(payload))
    .toString("base64url");
  const sig = crypto.createHmac("sha256", process.env.LAB_SSO_SECRET)
    .update(body).digest("hex");
  return `https://analytics.statspack.africa/sso?token=${body}.${sig}`;
}
```

Dart equivalent (server side only), using `crypto` and `dart:convert`:

```dart
final payload = jsonEncode({
  'api_key': apiKey, 'email': email, 'name': name,
  'exp': DateTime.now().millisecondsSinceEpoch ~/ 1000 + 120,
  'nonce': randomHex(8),
});
final body = base64Url.encode(utf8.encode(payload)).replaceAll('=', '');
final sig = Hmac(sha256, utf8.encode(secret)).convert(utf8.encode(body)).toString();
final url = 'https://analytics.statspack.africa/sso?token=$body.$sig';
```

## 4. Using it in the iframe

Ask your backend for a fresh URL each time the user opens **Reports → Analytics Lab**,
then load it:

```dart
final url = await api.getAnalyticsLabUrl();   // calls the Cloud Function above
// register the iframe with that src, exactly as you do now
```

Add `&embed=0` if you ever want the full standalone interface instead of the embedded one.

> **Generate the token on the server, never in the Flutter app.** Anything shipped in the
> app can be extracted, and the secret would let anyone mint tokens for any user. The
> Cloud Function pattern above keeps the secret server side.

Tokens are single-use and short-lived, so a URL captured from logs or history is already
useless by the time anyone sees it.

## 5. What Analytics Lab does on receipt

1. Verifies the HMAC signature against `SSO_SECRET`
2. Checks `exp` is in the future and not unreasonably far ahead
3. Checks the `nonce` has not been used before (replay protection)
4. Resolves `api_key` to a client, and rejects suspended clients
5. Finds the user by email, or creates them as an admin **for that client only**
6. Starts a session and loads the dashboard

Any failure shows a short message with the reason and a link to sign in normally, so
problems are easy to diagnose rather than silent.

## 6. Checking it works

```bash
curl -i "https://analytics.statspack.africa/sso?token=<your token>"
```

A valid token returns an HTML page containing "Signing you in…". An invalid one names the
exact problem — wrong secret, expired, already used, unknown api_key.
