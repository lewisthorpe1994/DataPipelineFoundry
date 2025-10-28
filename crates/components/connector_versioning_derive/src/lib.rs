use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use syn::spanned::Spanned;
use syn::{
    parse_macro_input, Attribute, Data, DeriveInput, Expr, ExprArray, Fields, GenericArgument, Lit,
    LitStr, PathArguments, Type,
};

#[derive(Clone)]
struct AllowedValueEntry {
    version: (u8, u8),
    values: Vec<String>,
}

struct FieldMeta {
    serde_key: String,
    compat: TokenStream2,
    allowed: Vec<AllowedValueEntry>,
    ident: syn::Ident,
    ty: Type,
    inner_ty: Option<Type>,
    is_option: bool,
    is_version: bool,
    skip: bool,
}

#[proc_macro_derive(ConnectorVersioned, attributes(compat, allowed_values, parser))]
pub fn derive_connector_versioned(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let ident = input.ident;
    let generics = input.generics;

    let parser_error = match parse_parser_error(&input.attrs) {
        Ok(path) => path,
        Err(err) => return err.to_compile_error().into(),
    };

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

    let mut field_meta: Vec<FieldMeta> = Vec::new();

    for field in named.named {
        let field_ident = field.ident.expect("named field");
        let serde = match parse_serde_info(&field.attrs) {
            Ok(info) => info,
            Err(err) => return err.to_compile_error().into(),
        };
        let serde_key = serde
            .rename
            .clone()
            .unwrap_or_else(|| field_ident.to_string());

        let is_version = is_version_type(&field.ty);
        let inner_ty = option_inner_type(&field.ty).cloned();
        let is_option = inner_ty.is_some();

        let compat_tokens = match find_compat(&field.attrs) {
            Ok(Some(ts)) => ts,
            Ok(None) => quote!(::connector_versioning::Compat::Always),
            Err(e) => return e.to_compile_error().into(),
        };

        let allowed = if serde.skip || is_version {
            Vec::new()
        } else {
            match find_allowed_values(&field.attrs) {
                Ok(values) => values,
                Err(err) => return err.to_compile_error().into(),
            }
        };

        field_meta.push(FieldMeta {
            serde_key,
            compat: compat_tokens,
            allowed,
            ident: field_ident,
            ty: field.ty.clone(),
            inner_ty,
            is_option,
            is_version,
            skip: serde.skip,
        });
    }

    let compat_entries = field_meta
        .iter()
        .filter(|meta| !meta.skip && !meta.is_version)
        .map(|meta| {
            let key_lit = LitStr::new(&meta.serde_key, Span::call_site());
            let compat_ts = &meta.compat;
            quote! { (#key_lit, #compat_ts) }
        });

    let allowed_specs: Vec<(TokenStream2, TokenStream2)> = field_meta
        .iter()
        .filter(|meta| !meta.allowed.is_empty())
        .map(|meta| {
            let const_ident =
                format_ident!("__ALLOWED_VALUES_{}", meta.ident.to_string().to_uppercase());

            let value_specs = meta.allowed.iter().map(|entry| {
                let major = entry.version.0;
                let minor = entry.version.1;
                let value_literals = entry
                    .values
                    .iter()
                    .map(|v| LitStr::new(v, Span::call_site()));
                quote! {
                    ::connector_versioning::ValueSpec {
                        version: ::connector_versioning::Version { major: #major, minor: #minor },
                        values: &[ #( #value_literals ),* ],
                    }
                }
            });

            let const_def = quote! {
                const #const_ident: &[::connector_versioning::ValueSpec] = &[
                    #(#value_specs),*
                ];
            };

            let key_lit = LitStr::new(&meta.serde_key, Span::call_site());
            let table_entry = quote! { (#key_lit, #const_ident) };

            (const_def, table_entry)
        })
        .collect();

    let (allowed_const_defs, allowed_table_entries): (Vec<_>, Vec<_>) =
        allowed_specs.into_iter().unzip();

    let field_allowed_values_impl = if allowed_table_entries.is_empty() {
        quote! {
            fn field_allowed_values(
            ) -> &'static [(&'static str, &'static [::connector_versioning::ValueSpec])] {
                &[]
            }
        }
    } else {
        quote! {
            fn field_allowed_values(
            ) -> &'static [(&'static str, &'static [::connector_versioning::ValueSpec])] {
                #(#allowed_const_defs)*
                static TABLE: &[(&str, &[::connector_versioning::ValueSpec])] = &[
                    #(#allowed_table_entries),*
                ];
                TABLE
            }
        }
    };

    let generated_empty_fields = field_meta.iter().map(|meta| {
        let ident = &meta.ident;
        if meta.is_version {
            quote! { #ident: __version }
        } else if meta.is_option {
            quote! { #ident: None }
        } else {
            quote! { #ident: ::core::default::Default::default() }
        }
    });

    let parse_statements = field_meta.iter().filter(|meta| !meta.is_version && !meta.skip).map(|meta| {
        let ident = &meta.ident;
        let key_lit = LitStr::new(&meta.serde_key, Span::call_site());
        if meta.is_option {
            let inner_ty = meta
                .inner_ty
                .as_ref()
                .expect("Option type should have inner type");
            quote! {
                {
                    let __raw = __config.remove(#key_lit);
                    let __parsed = match __raw {
                        Some(__value) => {
                            match __value.parse::<#inner_ty>() {
                                Ok(parsed) => Some(parsed),
                                Err(_) => {
                                    return Err(#parser_error::validation_error(format!(
                                        "invalid value for `{}`",
                                        #key_lit
                                    )));
                                }
                            }
                        }
                        None => None,
                    };
                    __instance.#ident = __parsed;
                }
            }
        } else {
            let ty = &meta.ty;
            quote! {
                {
                    let __raw = __config.remove(#key_lit).ok_or_else(|| #parser_error::validation_error(format!(
                        "missing required field `{}`",
                        #key_lit
                    )))?;
                    let __parsed = __raw.parse::<#ty>().map_err(|_| #parser_error::validation_error(format!(
                        "invalid value for `{}`",
                        #key_lit
                    )))?;
                    __instance.#ident = __parsed;
                }
            }
        }
    });

    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let expanded = quote! {
          impl #impl_generics ::connector_versioning::ConnectorVersioned for #ident #ty_generics #where_clause {
              fn field_compat() -> &'static [(&'static str, ::connector_versioning::Compat)] {
                  static TABLE: &[(&str, ::connector_versioning::Compat)] = &[
                      #(#compat_entries),*
                  ];
                  TABLE
              }

              #field_allowed_values_impl
          }

          impl #impl_generics #ident #ty_generics #where_clause {
              fn generated_empty(
                  __version: ::connector_versioning::Version,
              ) -> Self {
                  Self {
                      #(#generated_empty_fields),*
                  }
              }

              pub fn generated_new(
                  mut __config: ::std::collections::HashMap<String, String>,
                  __version: ::connector_versioning::Version,
              ) -> Result<Self, #parser_error> {
                  let mut __instance = Self::generated_empty(__version);
                  #(#parse_statements)*

                  let mut __errors = __instance.validate_version(__instance.version());
                  __errors.extend(__instance.validate_allowed_values(__instance.version()));
                  if !__errors.is_empty() {
                      return Err(#parser_error::validation_error(__errors.join("\n")));
                  }

                  // if !__config.is_empty() {
                  //     let extras = __config.keys().cloned().collect::<Vec<_>>().join(", ");
                  //     return Err(#parser_error::validation_error(
                  //           format!("unknown config keys: {}", extras),
                  //     ));
                  // }

                  Ok(__instance)
              }
          }
      };

    TokenStream::from(expanded)
}

struct SerdeInfo {
    rename: Option<String>,
    skip: bool,
}

fn parse_serde_info(attrs: &[Attribute]) -> syn::Result<SerdeInfo> {
    let mut info = SerdeInfo {
        rename: None,
        skip: false,
    };

    for attr in attrs {
        if !attr.path().is_ident("serde") {
            continue;
        }

        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename") {
                let lit: LitStr = meta.value()?.parse()?;
                info.rename = Some(lit.value());
                return Ok(());
            }
            if meta.path.is_ident("skip")
                || meta.path.is_ident("skip_serializing")
                || meta.path.is_ident("skip_deserializing")
            {
                info.skip = true;
                return Ok(());
            }
            if meta.path.is_ident("skip_serializing_if") {
                // Consume the expression but do not mark the field as skipped for parsing.
                let _ignored: syn::Expr = meta.value()?.parse()?;
                return Ok(());
            }
            Ok(())
        })?;
    }

    Ok(info)
}

fn option_inner_type(ty: &Type) -> Option<&Type> {
    if let Type::Path(type_path) = ty {
        if let Some(segment) = type_path.path.segments.first() {
            if segment.ident == "Option" {
                if let PathArguments::AngleBracketed(args) = &segment.arguments {
                    if let Some(GenericArgument::Type(inner_ty)) = args.args.first() {
                        return Some(inner_ty);
                    }
                }
            }
        }
    }
    None
}

fn is_version_type(ty: &Type) -> bool {
    if let Type::Path(type_path) = ty {
        if let Some(segment) = type_path.path.segments.last() {
            return segment.ident == "Version";
        }
    }
    false
}

fn parse_parser_error(attrs: &[Attribute]) -> syn::Result<syn::Path> {
    for attr in attrs {
        if !attr.path().is_ident("parser") {
            continue;
        }
        let mut error_path: Option<syn::Path> = None;
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("error") {
                let path: syn::Path = meta.value()?.parse()?;
                error_path = Some(path);
                return Ok(());
            }
            Err(meta.error("unknown parser key; expected `error`"))
        })?;

        return error_path.ok_or_else(|| {
            syn::Error::new(attr.span(), "parser attribute requires `error = \"...\"`")
        });
    }

    Err(syn::Error::new(
        Span::call_site(),
        "missing #[parser(error = \"...\")] attribute",
    ))
}

fn find_compat(attrs: &[Attribute]) -> syn::Result<Option<TokenStream2>> {
    for attr in attrs {
        if !attr.path().is_ident("compat") {
            continue;
        }

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

fn find_allowed_values(attrs: &[Attribute]) -> syn::Result<Vec<AllowedValueEntry>> {
    let mut out = Vec::new();
    for attr in attrs {
        if !attr.path().is_ident("allowed_values") {
            continue;
        }

        let mut range: Option<((u8, u8), (u8, u8))> = None;
        let mut version: Option<(u8, u8)> = None;
        let mut values: Option<Vec<String>> = None;

        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("range") {
                let lit: LitStr = meta.value()?.parse()?;
                let (min, max) = parse_range(&lit)?;
                range = Some((min, max));
                return Ok(());
            }
            if meta.path.is_ident("version") {
                let lit: LitStr = meta.value()?.parse()?;
                version = Some(parse_version(&lit)?);
                return Ok(());
            }
            if meta.path.is_ident("values") {
                let expr: ExprArray = meta.value()?.parse()?;
                let mut vals = Vec::new();
                for element in expr.elems.iter() {
                    match element {
                        Expr::Lit(lit) => match &lit.lit {
                            Lit::Str(s) => vals.push(s.value()),
                            _ => {
                                return Err(syn::Error::new_spanned(
                                    element,
                                    "allowed_values expects string literals",
                                ));
                            }
                        },
                        _ => {
                            return Err(syn::Error::new_spanned(
                                element,
                                "allowed_values expects string literals",
                            ));
                        }
                    }
                }
                values = Some(vals);
                return Ok(());
            }

            Err(meta.error("unknown allowed_values key; expected `range`, `version`, or `values`"))
        })?;

        let values = values.ok_or_else(|| {
            syn::Error::new_spanned(attr, "allowed_values requires a `values = [...]` entry")
        })?;

        let mut versions = Vec::new();
        match (range, version) {
            (Some((min, max)), None) => versions.extend(expand_versions(min, max)),
            (None, Some(ver)) => versions.push(ver),
            (Some(_), Some(_)) => {
                return Err(syn::Error::new_spanned(
                    attr,
                    "allowed_values: specify only one of `range` or `version`",
                ));
            }
            (None, None) => {
                return Err(syn::Error::new_spanned(
                    attr,
                    "allowed_values requires either `range` or `version`",
                ));
            }
        }

        for (major, minor) in versions {
            out.push(AllowedValueEntry {
                version: (major, minor),
                values: values.clone(),
            });
        }
    }

    Ok(out)
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

fn expand_versions(min: (u8, u8), max: (u8, u8)) -> Vec<(u8, u8)> {
    let mut out = Vec::new();
    let mut current = min;

    loop {
        out.push(current);
        if current == max {
            break;
        }

        let (mut major, mut minor) = current;
        if minor == u8::MAX {
            major = major.wrapping_add(1);
            minor = 0;
        } else {
            minor = minor.wrapping_add(1);
        }

        current = (major, minor);

        if (major, minor) > max {
            break;
        }
    }

    out
}
