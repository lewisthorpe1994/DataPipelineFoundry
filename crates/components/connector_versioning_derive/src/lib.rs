use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Attribute, Data, DeriveInput, Fields, LitStr};

#[proc_macro_derive(ConnectorVersioned, attributes(compat))]
pub fn derive_connector_versioned(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let ident = input.ident;
    let generics = input.generics;

    // Gather (serde_key, compat_expr) for each field
    let mut entries: Vec<(String, proc_macro2::TokenStream)> = Vec::new();

    let Data::Struct(ds) = input.data else {
        return syn::Error::new_spanned(&ident, "ConnectorVersioned only supports structs")
            .to_compile_error()
            .into();
    };

    let Fields::Named(named) = ds.fields else {
        return syn::Error::new_spanned(&ident, "ConnectorVersioned requires named fields")
            .to_compile_error()
            .into();
    };

    for field in named.named {
        let field_ident = field.ident.expect("named field");
        // Pick the serde rename if present; otherwise use the field name
        let serde_key = find_serde_rename(&field.attrs).unwrap_or_else(|| field_ident.to_string());

        // Parse #[compat(...)] on this field
        let compat_tokens = match find_compat(&field.attrs) {
            Ok(Some(ts)) => ts,
            Ok(None) => quote!(::connector_versioning::Compat::Always),
            Err(e) => return e.to_compile_error().into(),
        };

        entries.push((serde_key, compat_tokens));
    }

    let table_entries = entries.iter().map(|(k, ct)| {
        let k_lit = k.clone();
        quote! { (#k_lit, #ct) }
    });

    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let expanded = quote! {
        impl #impl_generics ::connector_versioning::ConnectorVersioned for #ident #ty_generics #where_clause {
            fn field_compat() -> &'static [(&'static str, ::connector_versioning::Compat)] {
                static TABLE: &[(&str, ::connector_versioning::Compat)] = &[
                    #(#table_entries),*
                ];
                TABLE
            }
        }
    };

    TokenStream::from(expanded)
}

/// Find `#[serde(rename = "...")]` and return the value if present.
fn find_serde_rename(attrs: &[Attribute]) -> Option<String> {
    let mut out: Option<String> = None;
    for attr in attrs {
        if !attr.path().is_ident("serde") {
            continue;
        }
        // syn v2: use parse_nested_meta
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename") {
                let lit: LitStr = meta.value()?.parse()?;
                out = Some(lit.value());
            }
            Ok(())
        });
    }
    out
}

/// Parse `#[compat(...)]` into a Compat token stream.
/// Supported:
/// - `#[compat(always)]`
/// - `#[compat(since = "3.1")]`
/// - `#[compat(until = "3.0")]`
/// - `#[compat(range = "3.0..=3.2")]`
fn find_compat(attrs: &[Attribute]) -> syn::Result<Option<proc_macro2::TokenStream>> {
    for attr in attrs {
        if !attr.path().is_ident("compat") {
            continue;
        }

        // State: only allow one of always/since/until/range
        #[derive(Default)]
        struct State {
            always: bool,
            since: Option<(u8, u8)>,
            until: Option<(u8, u8)>,
            range: Option<((u8, u8), (u8, u8))>,
        }
        let mut st = State::default();

        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("always") {
                st.always = true;
                return Ok(());
            }
            if meta.path.is_ident("since") {
                let lit: LitStr = meta.value()?.parse()?;
                st.since = Some(parse_version(&lit)?);
                return Ok(());
            }
            if meta.path.is_ident("until") {
                let lit: LitStr = meta.value()?.parse()?;
                st.until = Some(parse_version(&lit)?);
                return Ok(());
            }
            if meta.path.is_ident("range") {
                let lit: LitStr = meta.value()?.parse()?;
                let (min, max) = parse_range(&lit)?;
                st.range = Some((min, max));
                return Ok(());
            }
            Err(meta.error("unknown compat key; expected `always`, `since`, `until`, or `range`"))
        })?;

        // Validate combinations
        let mut count = 0;
        if st.always {
            count += 1;
        }
        if st.since.is_some() {
            count += 1;
        }
        if st.until.is_some() {
            count += 1;
        }
        if st.range.is_some() {
            count += 1;
        }
        if count == 0 {
            // treat missing as Always
            return Ok(Some(quote!(::connector_versioning::Compat::Always)));
        }
        if count > 1 {
            return Err(syn::Error::new_spanned(
                attr,
                "compat: specify only one of `always`, `since`, `until`, or `range`",
            ));
        }

        let ts = if st.always {
            quote!(::connector_versioning::Compat::Always)
        } else if let Some((maj, min)) = st.since {
            quote!(::connector_versioning::Compat::Since(::connector_versioning::Version{ major: #maj, minor: #min }))
        } else if let Some((maj, min)) = st.until {
            quote!(::connector_versioning::Compat::Until(::connector_versioning::Version{ major: #maj, minor: #min }))
        } else if let Some(((min_maj, min_min), (max_maj, max_min))) = st.range {
            quote!(::connector_versioning::Compat::Range{
                min: ::connector_versioning::Version{ major: #min_maj, minor: #min_min },
                max: ::connector_versioning::Version{ major: #max_maj, minor: #max_min },
            })
        } else {
            unreachable!()
        };

        return Ok(Some(ts));
    }
    Ok(None)
}

fn parse_version(lit: &LitStr) -> syn::Result<(u8, u8)> {
    let s = lit.value();
    let mut it = s.split('.');
    let maj = it
        .next()
        .ok_or_else(|| err_at(lit, "expected MAJOR.MINOR"))?
        .parse::<u8>()
        .map_err(|_| err_at(lit, "invalid major"))?;
    let min = it
        .next()
        .ok_or_else(|| err_at(lit, "expected MAJOR.MINOR"))?
        .parse::<u8>()
        .map_err(|_| err_at(lit, "invalid minor"))?;
    if it.next().is_some() {
        return Err(err_at(lit, "unexpected extra version segments"));
    }
    Ok((maj, min))
}

fn parse_range(lit: &LitStr) -> syn::Result<((u8, u8), (u8, u8))> {
    let s = lit.value().replace("..=", "..");
    let mut it = s.split("..");
    let a = it
        .next()
        .ok_or_else(|| err_at(lit, "missing min in range"))?;
    let b = it
        .next()
        .ok_or_else(|| err_at(lit, "missing max in range"))?;
    if it.next().is_some() {
        return Err(err_at(lit, "invalid range syntax"));
    }
    let a_lit = LitStr::new(a, lit.span());
    let b_lit = LitStr::new(b, lit.span());
    Ok((parse_version(&a_lit)?, parse_version(&b_lit)?))
}

fn err_at(lit: &LitStr, msg: &str) -> syn::Error {
    syn::Error::new(lit.span(), msg)
}
