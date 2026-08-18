## Default Permission

Allows the webview to make ConnectRPC calls.

All three commands are needed for a working transport: `connect_rpc` starts a
call, `connect_rpc_send` streams request messages into one, and
`connect_rpc_cancel` abandons one. Per-RPC access control belongs in the
services themselves, since the transport cannot tell one method from another.

#### This default permission set includes the following:

- `allow-connect-rpc`
- `allow-connect-rpc-send`
- `allow-connect-rpc-cancel`

## Permission Table

<table>
<tr>
<th>Identifier</th>
<th>Description</th>
</tr>


<tr>
<td>

`connectrpc-tauri:allow-connect-rpc`

</td>
<td>

Enables the connect_rpc command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`connectrpc-tauri:deny-connect-rpc`

</td>
<td>

Denies the connect_rpc command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`connectrpc-tauri:allow-connect-rpc-cancel`

</td>
<td>

Enables the connect_rpc_cancel command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`connectrpc-tauri:deny-connect-rpc-cancel`

</td>
<td>

Denies the connect_rpc_cancel command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`connectrpc-tauri:allow-connect-rpc-send`

</td>
<td>

Enables the connect_rpc_send command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`connectrpc-tauri:deny-connect-rpc-send`

</td>
<td>

Denies the connect_rpc_send command without any pre-configured scope.

</td>
</tr>
</table>
