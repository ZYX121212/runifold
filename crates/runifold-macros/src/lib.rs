//! Procedural macros for Runifold's typed Tool surface.

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    Expr, ExprLit, FnArg, GenericArgument, ItemFn, Lit, LitStr, MetaNameValue, PathArguments,
    ReturnType, Token, Type,
    parse::{Parse, ParseStream},
    parse_macro_input,
    punctuated::Punctuated,
};

/// Exposes an async typed Rust function as a Runifold Tool constructor.
///
/// The function must accept `(Input, ToolContext)` or
/// `(State<Service>, Input, ToolContext)`. It may return any error implementing
/// `IntoToolError`. `Input` must implement `DeserializeOwned` and `JsonSchema`;
/// ordinary `Output` values must implement `Serialize` and `JsonSchema`.
/// Set `output = "rich"` when the function returns `ToolOutput` containing
/// images, audio, documents, resources, or mixed content.
///
/// ```ignore
/// #[runifold::tool(
///     description = "Look up weather",
///     effect = "read_only",
///     risk = "low"
/// )]
/// async fn weather(
///     input: WeatherInput,
///     context: runifold::ToolContext,
/// ) -> Result<WeatherOutput, runifold::ToolError> {
///     // ...
/// }
///
/// let agent = client.agent("assistant", "model").tool(weather_tool()).build()?;
/// ```
#[proc_macro_attribute]
pub fn tool(attributes: TokenStream, item: TokenStream) -> TokenStream {
    let arguments = parse_macro_input!(attributes as ToolArguments);
    let function = parse_macro_input!(item as ItemFn);
    expand_tool(arguments, &function)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

#[derive(Default)]
struct ToolArguments {
    name: Option<LitStr>,
    description: Option<LitStr>,
    version: Option<LitStr>,
    effect: Option<LitStr>,
    risk: Option<LitStr>,
    output: Option<LitStr>,
}

impl Parse for ToolArguments {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let entries = Punctuated::<MetaNameValue, Token![,]>::parse_terminated(input)?;
        let mut arguments = Self::default();
        for entry in entries {
            let Some(identifier) = entry.path.get_ident() else {
                return Err(syn::Error::new_spanned(
                    entry.path,
                    "expected an identifier",
                ));
            };
            let value = string_literal(&entry.value)?;
            match identifier.to_string().as_str() {
                "name" => set_once(&mut arguments.name, value, identifier)?,
                "description" => {
                    set_once(&mut arguments.description, value, identifier)?;
                }
                "version" => set_once(&mut arguments.version, value, identifier)?,
                "effect" => set_once(&mut arguments.effect, value, identifier)?,
                "risk" => set_once(&mut arguments.risk, value, identifier)?,
                "output" => set_once(&mut arguments.output, value, identifier)?,
                _ => {
                    return Err(syn::Error::new_spanned(
                        identifier,
                        "supported keys are name, description, version, effect, risk, and output",
                    ));
                }
            }
        }
        Ok(arguments)
    }
}

fn expand_tool(
    arguments: ToolArguments,
    function: &ItemFn,
) -> syn::Result<proc_macro2::TokenStream> {
    if function.sig.asyncness.is_none() {
        return Err(syn::Error::new_spanned(
            function.sig.fn_token,
            "typed Tool functions must be async",
        ));
    }
    if !function.sig.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &function.sig.generics,
            "generic typed Tool functions are not supported",
        ));
    }
    let inputs = function.sig.inputs.iter().collect::<Vec<_>>();
    let (state_type, input_type) = match inputs.as_slice() {
        [input, context] => {
            let input = typed_argument(Some(input), "missing typed Tool input")?;
            let _context = typed_argument(Some(context), "missing ToolContext argument")?;
            (None, input)
        }
        [state, input, context] => {
            let state = typed_argument(Some(state), "missing State argument")?;
            let input = typed_argument(Some(input), "missing typed Tool input")?;
            let _context = typed_argument(Some(context), "missing ToolContext argument")?;
            (Some(state_inner_type(state)?), input)
        }
        _ => {
            return Err(syn::Error::new_spanned(
                &function.sig.inputs,
                "typed Tool functions accept (Input, ToolContext) or \
                 (State<Service>, Input, ToolContext)",
            ));
        }
    };
    let output_type = result_output(&function.sig.output)?;
    let description = arguments.description.ok_or_else(|| {
        syn::Error::new_spanned(
            &function.sig.ident,
            "typed Tool requires `description = \"...\"`",
        )
    })?;
    let function_name = &function.sig.ident;
    let constructor = format_ident!("{function_name}_tool");
    let visibility = &function.vis;
    let name = arguments
        .name
        .unwrap_or_else(|| LitStr::new(&function_name.to_string(), function_name.span()));
    let version = arguments
        .version
        .unwrap_or_else(|| LitStr::new("1", function_name.span()));
    let effect = effect_tokens(arguments.effect.as_ref())?;
    let risk = risk_tokens(arguments.risk.as_ref())?;
    let rich_output = output_mode(arguments.output.as_ref())?;
    let constructor_path = function_tool_constructor(input_type, output_type, rich_output);

    let constructor = if let Some(state_type) = state_type {
        quote! {
            #visibility fn #constructor(
                state: ::std::sync::Arc<#state_type>,
            ) -> impl ::runifold::Tool {
                let state = ::runifold::State::from_shared(state);
                #constructor_path(
                    #name,
                    #description,
                    move |input: #input_type, context: ::runifold::ToolContext| {
                        let state = state.clone();
                        async move {
                            #function_name(state, input, context)
                                .await
                                .map_err(::runifold::IntoToolError::into_tool_error)
                        }
                    },
                )
                .version(#version)
                .effect(#effect)
                .risk(#risk)
            }
        }
    } else {
        quote! {
            #visibility fn #constructor() -> impl ::runifold::Tool {
                #constructor_path(
                    #name,
                    #description,
                    |input: #input_type, context: ::runifold::ToolContext| async move {
                        #function_name(input, context)
                            .await
                            .map_err(::runifold::IntoToolError::into_tool_error)
                    },
                )
                .version(#version)
                .effect(#effect)
                .risk(#risk)
            }
        }
    };

    Ok(quote! {
        #function
        #constructor
    })
}

fn function_tool_constructor(input: &Type, output: &Type, rich: bool) -> proc_macro2::TokenStream {
    if rich {
        quote!(::runifold::FunctionTool::<#input, ::runifold::ToolOutput, _>::new_rich)
    } else {
        quote!(::runifold::FunctionTool::<#input, #output, _>::new)
    }
}

fn string_literal(expression: &Expr) -> syn::Result<LitStr> {
    if let Expr::Lit(ExprLit {
        lit: Lit::Str(value),
        ..
    }) = expression
    {
        Ok(value.clone())
    } else {
        Err(syn::Error::new_spanned(
            expression,
            "expected a string literal",
        ))
    }
}

fn set_once(
    target: &mut Option<LitStr>,
    value: LitStr,
    identifier: &syn::Ident,
) -> syn::Result<()> {
    if target.replace(value).is_some() {
        Err(syn::Error::new_spanned(
            identifier,
            "duplicate Tool attribute",
        ))
    } else {
        Ok(())
    }
}

fn typed_argument<'a>(argument: Option<&'a FnArg>, message: &str) -> syn::Result<&'a Type> {
    match argument {
        Some(FnArg::Typed(argument)) => Ok(&argument.ty),
        Some(argument) => Err(syn::Error::new_spanned(
            argument,
            "methods cannot be exposed as typed Tools",
        )),
        None => Err(syn::Error::new(proc_macro2::Span::call_site(), message)),
    }
}

fn result_output(output: &ReturnType) -> syn::Result<&Type> {
    let ReturnType::Type(_, output) = output else {
        return Err(syn::Error::new_spanned(
            output,
            "typed Tool must return Result<Output, ToolError>",
        ));
    };
    let Type::Path(path) = output.as_ref() else {
        return Err(syn::Error::new_spanned(
            output,
            "typed Tool must return Result<Output, ToolError>",
        ));
    };
    let Some(result) = path.path.segments.last() else {
        return Err(syn::Error::new_spanned(output, "missing Result output"));
    };
    if result.ident != "Result" {
        return Err(syn::Error::new_spanned(
            result,
            "typed Tool must return Result<Output, ToolError>",
        ));
    }
    let PathArguments::AngleBracketed(arguments) = &result.arguments else {
        return Err(syn::Error::new_spanned(
            result,
            "Result requires output and error types",
        ));
    };
    arguments
        .args
        .iter()
        .find_map(|argument| match argument {
            GenericArgument::Type(output) => Some(output),
            _ => None,
        })
        .ok_or_else(|| syn::Error::new_spanned(arguments, "Result output type is missing"))
}

fn state_inner_type(state: &Type) -> syn::Result<&Type> {
    let Type::Path(path) = state else {
        return Err(syn::Error::new_spanned(
            state,
            "first injected argument must be State<Service>",
        ));
    };
    let Some(segment) = path.path.segments.last() else {
        return Err(syn::Error::new_spanned(
            state,
            "first injected argument must be State<Service>",
        ));
    };
    if segment.ident != "State" {
        return Err(syn::Error::new_spanned(
            state,
            "first injected argument must be State<Service>",
        ));
    }
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return Err(syn::Error::new_spanned(
            segment,
            "State requires one service type",
        ));
    };
    let mut types = arguments.args.iter().filter_map(|argument| match argument {
        GenericArgument::Type(value) => Some(value),
        _ => None,
    });
    let inner = types
        .next()
        .ok_or_else(|| syn::Error::new_spanned(arguments, "State service type is missing"))?;
    if types.next().is_some() {
        return Err(syn::Error::new_spanned(
            arguments,
            "State accepts exactly one service type",
        ));
    }
    Ok(inner)
}

fn effect_tokens(effect: Option<&LitStr>) -> syn::Result<proc_macro2::TokenStream> {
    let value = effect.map_or_else(|| "pure".to_owned(), LitStr::value);
    let variant = match value.as_str() {
        "pure" => quote!(Pure),
        "read_only" => quote!(ReadOnly),
        "idempotent_write" => quote!(IdempotentWrite),
        "non_idempotent_write" => quote!(NonIdempotentWrite),
        "destructive" => quote!(Destructive),
        "unknown" => quote!(Unknown),
        _ => {
            return Err(syn::Error::new_spanned(
                effect.expect("invalid values are present"),
                "invalid effect; expected pure, read_only, idempotent_write, \
                 non_idempotent_write, destructive, or unknown",
            ));
        }
    };
    Ok(quote!(::runifold::core::EffectClass::#variant))
}

fn risk_tokens(risk: Option<&LitStr>) -> syn::Result<proc_macro2::TokenStream> {
    let value = risk.map_or_else(|| "low".to_owned(), LitStr::value);
    let variant = match value.as_str() {
        "low" => quote!(Low),
        "medium" => quote!(Medium),
        "high" => quote!(High),
        "critical" => quote!(Critical),
        _ => {
            return Err(syn::Error::new_spanned(
                risk.expect("invalid values are present"),
                "invalid risk; expected low, medium, high, or critical",
            ));
        }
    };
    Ok(quote!(::runifold::core::RiskLevel::#variant))
}

fn output_mode(output: Option<&LitStr>) -> syn::Result<bool> {
    match output.map(LitStr::value).as_deref() {
        None | Some("json") => Ok(false),
        Some("rich") => Ok(true),
        Some(_) => Err(syn::Error::new_spanned(
            output.expect("invalid values are present"),
            "invalid output; expected json or rich",
        )),
    }
}
