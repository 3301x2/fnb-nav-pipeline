# Message for Sipho — BI_SANDBOX access on Mac

Copy the blockquote below into Teams.

---

> Hi Sipho,
>
> I've been struggling to use BI_SANDBOX on RSD-RBSQLDEV from my Mac and I want to make sure I'm set up properly before next month-end.
>
> **The problem**
> - On VPN I can reach the server (TCP 1433 opens), so the network path is fine.
> - All four auth attempts fail with `Login failed for user 'FNBJNB01\f8887375'` (SQL Server error 18456).
> - I've also tried Kerberos / Windows-integrated auth from the Mac — same rejection.
>
> **What I need to confirm with you**
>
> 1. Has my AD account (`f8887375`) been granted a SQL Server login on this instance, and `db_datareader` (or whatever the right role is) on `BI_SANDBOX`?
>
> 2. Does the server accept SQL Server authentication (username + password), or is it Windows-integrated auth only? Mac clients can't do integrated Windows auth without Kerberos setup, and even with that, the SPN has to be registered. If it's Windows-only, can I either:
>     - Get a SQL Server login (username + password) created for me, or
>     - Get the realm name, KDC hostnames, and SPN so I can configure Kerberos on my end?
>
> 3. What data lands in `BI_SANDBOX` and how? Is it the monthly base/transaction tables that match `customer_spend.base_data` / `customer_spend.transaction_data` in BigQuery? Right now we're pulling those via SFTP from `avalonwinscp` and converting them manually — if `BI_SANDBOX` already has them, we can skip a lot of that.
>
> Pierre's account works against `avalonwinscp` so I have a fallback for now, but I'd like to consolidate on the SQL path you set up. Happy to call if quicker.
>
> Thanks.
